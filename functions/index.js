const functions = require('firebase-functions');
const admin = require('firebase-admin');
const jwt = require('jsonwebtoken');
const request = require('request');
const rp = require('request-promise');
const md5 = require('js-md5');
const twilio_client = require('twilio')(functions.config().twilio.sid, functions.config().twilio.auth);
const jwtHeaderName = "x-extension-jwt";
const launchesRoot = "launches";
const playersRoot = "players";
const tokensRoot = "tokens";
const upgradesRoot = "upgrades";
const transactionsRoot = "transactions";
const twitchAuraRoute = "store/twitchaura/exclusiveTo";
const usageRoot = "usage";
const bitsExtensionVersion = "0.0.2";

admin.initializeApp();
var db = admin.database();

function InvalidHashException() {
    this.message = "Invalid hash given.";
}

function InternalServerErrorException() {
    this.message = "Server error.";
}

function verifyKeyHash(key, givenHash) {
    return md5(key).toUpperCase() === givenHash.toUpperCase();
}

function tryVerify(token, key) {
    try {
        var verification = jwt.verify(token, key);
        return true;
    }
    catch (err) {
        return false;
    }
}

var verifyJwt = function(token) {
    if (token === undefined) {
        return [false, 401, "Missing signed JWT."];
    }
    
    var encodedKey = functions.config().twitch.key;
    
    if (encodedKey === undefined) {
        return [false, 500, "Internal error."];
    }
    
    var key = Buffer.from(encodedKey, 'base64');
    if (tryVerify(token, key) === false) {
        // first key failed, try with second key
        var encodedKey2 = functions.config().twitch.key2;
        if (encodedKey2 === undefined) {
            return [false, 500, "Internal error."];
        }
        
        key = Buffer.from(encodedKey2, 'base64');
        if (tryVerify(token, key) === false) {
            // both tokens failed
            console.log("Sending status 401. Could not verify JWT."); // DEBUG
            return [false, 401, "Sending status 401. Could not verify JWT"];
        }
    }
    
    return [true];
};

function sendPubSubBroadcast(encodedKey, clientId, channelId, payload) {
        // send pubsub message with update
        // generate and sign JWT
        if (encodedKey === undefined || clientId === undefined) {
            console.log("Sending status 500. Could not find twitch key or client ID");
            throw new InternalServerErrorException();
        }

        var token = {
            "exp": Date.now() + 60,
            "role":"external",
            "channel_id": channelId.trim(),
            "pubsub_perms": {
                send: ["*"]
            }
        };
        
        var signedToken = jwt.sign(token, Buffer.from(encodedKey, 'base64'), { 'noTimestamp': true });
        var messageText = JSON.stringify(payload);
    
        // send PubSub message
        var options = {
            method: 'POST',
            uri: 'https://api.twitch.tv/extensions/message/' + channelId.trim(),
            auth: {
                'bearer': signedToken
            },
            headers: {
                "Client-ID": clientId
            },
            body: {
                "content_type": "application/json",
                "message": messageText,
                "targets": ["broadcast"]
            },
            json: true // Automatically stringifies the body to JSON
        };

        return rp(options); 
}

function trySendTransactionChatMessage(encodedKey, clientId, channelId, messageText) {
    try {
        return sendTransactionChatMessage(encodedKey, clientId, channelId, messageText);
    }
    catch (err) {
        return undefined;
    }
}

function sendTransactionChatMessage(encodedKey, clientId, channelId, messageText) {
        // send chat message
        // generate and sign JWT
        if (encodedKey === undefined || clientId === undefined) {
            console.log("Sending status 500. Could not find twitch key or client ID");
            throw new InternalServerErrorException();
        }

        var token = {
            "exp": Date.now() + 60,
            "role":"external",
            "user_id": channelId.trim(),
            "channel_id": channelId.trim()
        };
        
        var signedToken = jwt.sign(token, Buffer.from(encodedKey, 'base64'), { 'noTimestamp': true });

        // send extension message
        var options = {
            method: 'POST',
            uri: `https://api.twitch.tv/extensions/${clientId}/${bitsExtensionVersion}/channels/${channelId.trim()}/chat`,
            // auth: {
            //     'Bearer': signedToken
            // },
            headers: {
                "Authorization": "Bearer " + signedToken,
                "Client-ID": clientId,
                "Content-Type": "application/json"
            },
            body: `{"text": "${messageText}"}`
        };

        return rp(options);     
}

function generateUpgradeObj(transaction, playerId) {
    var puckCount;
    var target;
    var message;
    switch (transaction.product.sku) {
        case 'get-100':
            puckCount = 100;
            target = playerId;
            message = `${transaction.displayName} used ${transaction.product.cost.amount} ${transaction.product.cost.type} to get 100 pucks!`;
            break;
        case 'give-10-to-everyone':
            puckCount = 10;
            target = "all";
            message = `${transaction.displayName} used ${transaction.product.cost.amount} ${transaction.product.cost.type} to give everyone 10 pucks!`;
            break;
        case 'give-100-to-everyone':
            puckCount = 100;
            target = "all";
            message = `${transaction.displayName} used ${transaction.product.cost.amount} ${transaction.product.cost.type} to give everyone 100 pucks!`;
            break;
        default:
            return undefined;
    }

    return {
        puckCount: puckCount,
        source: playerId,
        target: target,
        message: message
    };
}

exports.queueLaunch = functions.https.onRequest((request, response) => {
    // CORS
    if (request.method === 'OPTIONS') {
        console.log("Sending status 200. CORS check successful."); // DEBUG
        return response.set('Access-Control-Allow-Origin', '*')
        .set('Access-Control-Allow-Methods', 'GET, POST')
        .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, x-extension-jwt')
        .status(200).send();
    }

    // verify JWT
    var verifyArr = verifyJwt(request.get(jwtHeaderName));
    if(verifyArr[0] !== true) {
        return response.status(verifyArr[1]).send(verifyArr[2]);
    }

    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id."); // DEBUG
        return response.status(400).send("Missing channel Id."); // channel Id parameter is missing
    }
    channelId = channelId.trim();

    // verify player Id is given
    var playerId = request.query.playerId;
    if (playerId === undefined) {
        console.log("Sending status 400. Missing player Id."); // DEBUG
        return response.status(400).send("Missing player Id");
    }
    playerId = playerId.trim();

    // verify json is correct
    var launchData = request.body;

    if (launchData.constructor !== Array) { // check if we were sent an array
        console.log("Sending status 400. The json object either didn't parse correctly or isn't an array of launch objects."); // DEBUG
        return response.status(400).send('Invalid JSON. Must be an array of launch objects.');
    }

    for (var i = 0; i < launchData.length; i++) {
        if (launchData[i] === undefined || launchData[i].id === undefined) {
            console.log("Sending status 400. Launch object was undefined or didn't have an id."); // DEBUG
            return response.status(400).send('Invalid JSON. Launch objects must have an id.');
        }
    }

    console.log("Received: " + JSON.stringify(launchData)); // DEBUG

    // update database, excluding launches that have 0 pucks or are undefined
    var launchPromises = [];
    for(i = 0; i < launchData.length; i++) {
        if (launchData[i].pucks !== undefined && launchData[i].pucks <= 0) {
            continue;
        }

        var ref = db.ref(`${launchesRoot}/${channelId.trim()}`);
        launchPromises.push(
            ref.push().set(launchData[i]).catch(reason => {
                console.log(reason);
                return response.sendStatus(500);
            })
        );
        console.log("Adding launch to database: " + JSON.stringify(launchData[i]));
    }

    if (launchPromises.length <= 0) {
        console.log("No launch objects committed to database.");
        return response.set('Access-Control-Allow-Origin', '*').sendStatus(200);
    }

    return Promise.all(launchPromises).then((snapshot) => {
        console.log("All launches committed to database.");
        return response.set('Access-Control-Allow-Origin', '*').sendStatus(200);
    });
});

exports.wildUserAppears = functions.https.onRequest((request, response) => {
    // send CORS first
    if (request.method === 'OPTIONS') {
        console.log("Sending status 200. CORS check successful."); // DEBUG
        return response.set('Access-Control-Allow-Origin', '*')
            .set('Access-Control-Allow-Methods', 'GET, POST')
            .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, x-extension-jwt')
            .status(200).send();
    }
    
    // verify JWT
    var verifyArr = verifyJwt(request.get(jwtHeaderName));
    if (verifyArr[0] !== true) {
        return response.status(verifyArr[1]).send(verifyArr[2]);
    }
    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id."); // DEBUG
        return response.status(400).send("Missing channel Id."); // channel Id parameter is missing
    }
    // verify player Id is given
    var playerId = request.query.playerId;
    if (playerId === undefined) {
        console.log("Sending status 400. Missing player Id."); // DEBUG
        return response.status(400).send("Missing player Id");
    }
    // verify opaque user Id is given
    var opaqueUserId = request.query.opaqueUserId;
    if (opaqueUserId === undefined) {
        console.log("Sending status 400. Missing opaque User Id."); // DEBUG
        return response.status(400).send("Missing opaque User Id");
    }
    //initialize new user data
    var channelRef = db.ref(`${playersRoot}/${channelId.trim()}`);
    var puckCount = 100;
    var points = 0;
    
    var playerRef = db.ref(`${playersRoot}/${channelId.trim()}/${playerId}`);
    return playerRef.once('value').then(snapshot => {
        if (snapshot.val() === null) {
            return playerRef.set({
                'points': points,
                'puckCount': puckCount,
                'opaqueUserId': opaqueUserId,
                'lastSeen': Date.now(),
                'itemsPurchased': [{ 'default': 'default' }]
                });
        }
        else {
            puckCount = snapshot.val().puckCount;
            points = snapshot.val().points;
            return playerRef.update({ 'lastSeen': Date.now() });
        }
    }).then(snapshot => {
        var responseBody = {
            'puckCount': puckCount,
            'points': points
        };
        console.log(JSON.stringify({'opaqueUserId': opaqueUserId, 'puckCount': puckCount, 'points': points}));
        return response.set('Access-Control-Allow-Origin', '*')
        .json(responseBody);
    }).catch(reason => {
        console.log(reason);
        return response.sendStatus(500);
    });
});

exports.verifyToken = functions.https.onRequest((request, response) => {
    // verify twitch auth token is given
    var token = request.header("Authorization");
    if (token === undefined || token === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }

    // verify token with twitch
    var options = {
        method: 'GET',
        uri: 'https://id.twitch.tv/oauth2/validate',
        headers: {
            "Authorization": "OAuth " + token
        },
        json: true
    };
    
    var tokenSalt = functions.config().streampucks.tokensalt;

    return rp(options).then((body) => {
        if ("login" in body && "user_id" in body) {
            // save the token hash
            var ref = db.ref(`${tokensRoot}/${body.user_id}`);
            var hashObj = {
                hash: md5(body.user_id + token + tokenSalt),
                lastValidated: Date.now()                
            };
            return ref.set(hashObj).then(((resBody) => {
                hashObj.login = body.login;
                hashObj.user_id = body.user_id;
                return response.status(200).send(JSON.stringify(hashObj));
            })).catch((errBody) => {
                console.log("Failed to set token hash.");
                console.log(err.message);
                return response.status(500).status("Server error.");
            });
        }

        return response.status(400).send("Auth token invalid.");
    }).catch((err) => {
        console.log(err.message);
        return response.status(500).send("Server error.");
    });
});

exports.deleteUpgrades = functions.https.onRequest((request, response) => {
    // verify hash was given
    var givenHash = request.header("Authorization");
    if (givenHash === undefined || givenHash === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }

    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id.");
        return response.status(400).send("Missing channel Id.");
    }

    // verify json is correct
    if (request.body.hasOwnProperty('upgradeids') === false || request.body.upgradeids.constructor !== Array) { // check if we were sent an array
        console.log("Sending status 400. Invalid JSON.");
        console.log(request.body); // DEBUG
        return response.status(400).send('Invalid JSON. Must be an array of upgrade ids.');
    }

    var upgradeIds = request.body.upgradeids;

    // verify hash is valid
    var hashRef = db.ref(`${tokensRoot}/${channelId}/hash`);
    var upgradesRef = db.ref(`${upgradesRoot}/${channelId}`);
    return hashRef.once('value').then((snapshot) => {
        var hash = snapshot.val();
        if (givenHash === hash) {
            return upgradesRef.once('value');
        }

        throw new InvalidHashException();
    }).then((snapshot) => { // hash is valid, delete upgrades
        var updates = {};
        for (var key in snapshot.val()) {
            if (upgradeIds.indexOf(key) !== -1) {
                updates[key] = null;
            }
        }

        return upgradesRef.update(updates);
    }).then((snapshot) => {
        return response.sendStatus(200);
    }).catch((err) => {
        console.log(err.message);
        if (err.message === "Invalid hash given.") {
            return response.sendStatus(401);
        }
        else {
            return response.sendStatus(500);
        }
    });
});

exports.deleteLaunches = functions.https.onRequest((request, response) => {
    // verify hash was given
    var givenHash = request.header("Authorization");
    if (givenHash === undefined || givenHash === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }

    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id.");
        return response.status(400).send("Missing channel Id.");
    }

    // verify json is correct
    var deleteAll = false;
    if (request.body.hasOwnProperty('deleteAll') && request.body.deleteAll === true) {
        deleteAll = true;
    }
    else if (request.body.hasOwnProperty('launchids') && request.body.launchids.constructor !== Array) { // check if we were sent an array
        console.log("Sending status 400. Invalid JSON.");
        console.log(request.body); // DEBUG
        return response.status(400).send('Invalid JSON. Must be an array of launch ids.');
    }
    else if (request.body.hasOwnProperty('deleteAll') === false && request.body.hasOwnProperty('launchids') === false) {
        console.log("Sending status 400. Invalid JSON.");
        console.log(request.body); // DEBUG
        return response.status(400).send('Invalid JSON. Must be an array of launch ids.');
    }

    var launchIds = request.body.launchids;

    // verify hash is valid
    var hashRef = db.ref(`${tokensRoot}/${channelId}/hash`);
    var launchesRef = db.ref(`${launchesRoot}/${channelId}`);
    return hashRef.once('value').then((snapshot) => {
        var hash = snapshot.val();
        if (givenHash === hash) {
            return launchesRef.once('value');
        }

        throw new InvalidHashException();
    }).then((snapshot) => { // hash is valid, delete launches
        if (deleteAll) {
            console.log(`Deleting all launches for ${channelId}.`); // DEBUG
            return launchesRef.remove();
        }

        var updates = {};
        for (var key in snapshot.val()) {
            if (launchIds.indexOf(key) !== -1) {
                updates[key] = null;
            }
        }

        return launchesRef.update(updates);
    }).then((snapshot) => {
        return response.sendStatus(200);
    }).catch((err) => {
        console.log(err.message);
        if (err.message === "Invalid hash given.") {
            return response.sendStatus(401);
        }
        else {
            return response.sendStatus(500);
        }
    });
});

exports.updateUsers = functions.https.onRequest((request, response) => {
    // verify hash was given
    var givenHash = request.header("Authorization");
    if (givenHash === undefined || givenHash === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }

    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id.");
        return response.status(400).send("Missing channel Id.");
    }

    // verify body is valid and build updates
    var updatePromises = [];
    var givenPlayerInfo = request.body;
    var updates = {};
    if (givenPlayerInfo === undefined || givenPlayerInfo === "" || typeof givenPlayerInfo !== 'object') {
        console.log("Missing or malformed updated player info."); // DEBUG
        return response.status(400).send("Missing or malformed updated player info.");
    }

    for(var key in givenPlayerInfo) {
        var hasUpdate = false;
        var tmp = {};
        if (givenPlayerInfo[key].hasOwnProperty('puckCount')) {
            if (isNaN(givenPlayerInfo[key].puckCount)) {
                return response.status(400).send("Puck count must be a number.");
            }
            tmp.puckCount = givenPlayerInfo[key].puckCount;
            hasUpdate = true;
        }
        if (givenPlayerInfo[key].hasOwnProperty('points')) {
            if (isNaN(givenPlayerInfo[key].points)) {
                return response.status(400).send("Points must be a number.");
            }
            tmp.points = givenPlayerInfo[key].points;
            hasUpdate = true;
        }

        if (hasUpdate) {
            updates[key] = tmp;
            var tmpRef = db.ref(`${playersRoot}/${channelId}/${key}`);
            updatePromises.push(tmpRef.update(tmp));
        }
    }

    // verify hash is valid
    var hashRef = db.ref(`${tokensRoot}/${channelId}/hash`);
    var playersRef = db.ref(`${playersRoot}/${channelId}`);

    return hashRef.once('value').then((snapshot) => {
        var hash = snapshot.val();
        if (givenHash === hash) {
            //return playersRef.update(updates);
            return Promise.all(updatePromises);
        }

        throw new InvalidHashException();
    }).then((snapshot) => {
        // send first pubsub message to non-bits extension
        return sendPubSubBroadcast(functions.config().twitch.key,
                                   functions.config().twitch.id,
                                   channelId,
                                   updates);   
    }).then((snapshot) => {
        // wait one second to not exceed pubsub rate limit
        return new Promise((resolve, reject) => {
            setTimeout(resolve, 1000);
        });
    }).then((snapshot) => {
        // send second pubsub message (for bits extension)
        return sendPubSubBroadcast(functions.config().twitch.key2,
                                   functions.config().twitch.id2,
                                   channelId,
                                   updates);  
    }).then((snapshot) => {
        return response.sendStatus(200);
    }).catch((err) => {
        console.log(err.message);
        if (err.message === "Invalid hash given.") {
            return response.sendStatus(401);
        }
        else {
            return response.sendStatus(500);
        }
    });
});

exports.purchasePointsUpdate = functions.https.onRequest((request, response) => {
    // send CORS first
    if (request.method === 'OPTIONS') {
        console.log("Sending status 200. CORS check successful."); // DEBUG
        return response.set('Access-Control-Allow-Origin', '*')
            .set('Access-Control-Allow-Methods', 'GET, POST')
            .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, x-extension-jwt')
            .status(200).send();
    }

    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id."); // DEBUG
        return response.status(400).send("Missing channel Id."); // channel Id parameter is missing
    }
    // verify player Id is given
    var playerId = request.query.playerId;
    if (playerId === undefined) {
        console.log("Sending status 400. Missing player Id."); // DEBUG
        return response.status(400).send("Missing player Id");
    }
    // verify store item is given
    var storeItemId = request.query.storeItemId;
    if (storeItemId === undefined) {
        console.log("Sending status 400. Missing store Item."); // DEBUG
        return response.status(400).send("Missing store Item");
    }

    var pointTotal;
    var itemCost;
    var dbRef = db.ref();
    return dbRef.once('value').then(snapshot => {
        //verify store item exists
        if (!snapshot.child(`store/${storeItemId}`).exists()) {
            console.log("Invalid Store Item");
            return response.status(400).send("Invalid store item");
        }
        else if (snapshot.child(`${playersRoot}/${channelId}/${playerId}/itemsPurchased/${storeItemId}`).val() !== null) {
            console.log("Item already purchased");
            console.log(snapshot.child(`${playersRoot}/${channelId}/${playerId}/itemsPurchased/${storeItemId}`).val());
            return response.set('Access-Control-Allow-Origin', '*')
                .status(410).send("Item already Purchased");
        }
        else {
            itemCost = snapshot.child(`store/${storeItemId}/cost`).val();
        }
        pointTotal = snapshot.child(`${playersRoot}/${channelId}/${playerId}/points`).val();
        if (pointTotal >= itemCost) {
            pointTotal -= itemCost;
            var updates = {};
            updates[`${playersRoot}/${channelId}/${playerId}/points`] = pointTotal;
            updates[`${playersRoot}/${channelId}/${playerId}/itemsPurchased/` + storeItemId] = storeItemId;
            dbRef.update(updates);
            return response.set('Access-Control-Allow-Origin', '*')
                .status(200).send("" + pointTotal);
        }
        else {
            console.log("Not enough points");
            return response.set('Access-Control-Allow-Origin', '*')
                .status(400).send("Not enough points");
        }

    }).catch(reason => {
        console.log(reason);
        return response.sendStatus(500);
    });
});

exports.populateStoreItems = functions.https.onRequest((request, response) => {
    // send CORS first
    if (request.method === 'OPTIONS') {
        console.log("Sending status 200. CORS check successful."); // DEBUG
        return response.set('Access-Control-Allow-Origin', '*')
            .set('Access-Control-Allow-Methods', 'GET, POST')
            .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, x-extension-jwt')
            .status(200).send();
    }
    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id."); // DEBUG
        return response.status(400).send("Missing channel Id."); // channel Id parameter is missing
    }
    // verify player Id is given
    var playerId = request.query.playerId;
    if (playerId === undefined) {
        console.log("Sending status 400. Missing player Id."); // DEBUG
        return response.status(400).send("Missing player Id");
    }
    //// verify JWT
    //var verifyArr = verifyJwt(request.get(jwtHeaderName));
    //if (verifyArr[0] !== true) {
    //    return response.status(verifyArr[1]).send(verifyArr[2]);
    //}

    var itemsJSON;
    var purchasedItems;
    var dbRef = db.ref();
    return dbRef.once('value').then(snapshot => {
        itemsJSON = { store: snapshot.child('store').val() };
        purchasedItems = snapshot.child(`${playersRoot}/${channelId}/${playerId}/itemsPurchased`).val();
        if (purchasedItems !== undefined) {
            itemsJSON.itemsPurchased = purchasedItems;
        }
        return response.set('Access-Control-Allow-Origin', '*')
            .status(200).json(itemsJSON);
    }).catch(reason => {
        return response.set('Access-Control-Allow-Origin', '*')
            .status(500).send(reason);
    });
});

exports.logTransaction = functions.https.onRequest((request, response) => {
        // CORS
        if (request.method === 'OPTIONS') {
            console.log("Sending status 200. CORS check successful."); // DEBUG
            return response.set('Access-Control-Allow-Origin', '*')
            .set('Access-Control-Allow-Methods', 'GET, POST')
            .set('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, x-extension-jwt')
            .status(200).send();
        }
    
        // verify JWT
        var verifyArr = verifyJwt(request.get(jwtHeaderName));
        if(verifyArr[0] !== true) {
            return response.status(verifyArr[1]).send(verifyArr[2]);
        }
    
        // verify channel Id is given
        var channelId = request.query.channelId;
        if (channelId === undefined) {
            console.log("Sending status 400. Missing channel Id."); // DEBUG
            return response.status(400).send("Missing channel Id."); // channel Id parameter is missing
        }
        channelId = channelId.trim();

        // verify player Id is given
        var playerId = request.query.playerId;
        if (playerId === undefined) {
            console.log("Sending status 400. Missing player Id."); // DEBUG
            return response.status(400).send("Missing player Id");
        }
        playerId = playerId.trim();
    
        // verify json is correct
        var transactionData = request.body;

        if (transactionData === undefined ||
            transactionData.product === undefined ||
            transactionData.product.sku === undefined ||
            transactionData.transactionId === undefined) {
            console.log("Sending status 400. Malformed JSON."); // DEBUG
            console.log(transactionData); // DEBUG
            return response.status(400).send("Malformed transaction JSON object.");
        }

        var upgradeObj = generateUpgradeObj(transactionData, playerId);

        if (upgradeObj === undefined) {
            console.log("Unable to generate upgrade object from transaction data.") // DEBUG
            return response.sendStatus(500);
        }

        // store game update and store the transaction data
        var transactionObj = transactionData;
        transactionObj["time"] = Date.now();
        var upgradesRef = db.ref(`${upgradesRoot}/${channelId.trim()}`);
        var transactionsRef = db.ref(`${transactionsRoot}/${channelId}/${playerId}`);
        return upgradesRef.push().set(upgradeObj).then((snapshot) => {
            return transactionsRef.push().set(transactionObj);
        }).then((snapshot) => {
            // try to send a chat message indicating the purchase was made,
            // but if it fails, don't block the purchase
            var promise = trySendTransactionChatMessage(functions.config().twitch.key2,
                                                        functions.config().twitch.id2,
                                                        channelId,
                                                        upgradeObj.message);
            if (promise !== undefined) {
                return promise;
            }

            console.log("WARNING failed to send bits purchase chat message.");
            return Promise.resolve();
        }).then((snapshot) => {
            return response.set('Access-Control-Allow-Origin', '*').sendStatus(200);
        }).catch(reason => {
            console.log(reason);
            return response.sendStatus(500);
        })
});

exports.levelStarted = functions.https.onRequest((request, response) => {
    // verify hash was given
    var givenHash = request.header("Authorization");
    if (givenHash === undefined || givenHash === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }

    // verify channel Id is given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        console.log("Sending status 400. Missing channel Id.");
        return response.status(400).send("Missing channel Id.");
    }

    var gameMode = request.query.gameMode; // could be undefined

    // verify hash is valid
    var hashRef = db.ref(`${tokensRoot}/${channelId.trim()}/hash`);
    var usageRef = db.ref(`${usageRoot}/${channelId.trim()}`);

    return hashRef.once('value').then((snapshot) => {
        var hash = snapshot.val();
        if (givenHash === hash) {
            var usageObj = [];
            usageObj[Date.now()] = gameMode;
            return usageRef.set(usageObj);
        }

        throw new InvalidHashException();
    }).then((snapshot) => {
        return response.sendStatus(200);
    }).catch((err) => {
        console.log(err.message);
        if (err.message === "Invalid hash given.") {
            return response.sendStatus(401);
        }
        else {
            return response.sendStatus(500);
        }
    });
});

exports.getUsageData = functions.https.onRequest((request, response) => {
    // verify hash
    var givenHash = request.header("Authorization");
    if (givenHash === undefined || givenHash === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }
    
    if (verifyKeyHash(functions.config().twitch.key, givenHash) === false && verifyKeyHash(functions.config().twitch.key2, givenHash) === false){
        return response.status(400).send("Invalid Auth token");
    }

    var usageRoute = usageRoot;
    var channelId = request.query.channelId;
    if (channelId !== undefined && channelId !== "") {
        usageRoute += "/" + channelId;
    }

    db.ref(usageRoute).once('value').then((snapshot) => {
        return response.status(200).send(JSON.stringify(snapshot.val()));
    }).catch((err) => {
        console.log(err);
        return response.sendStatus(500);
    });
});

exports.unlockTwitchConTrail = functions.https.onRequest((request, response) => {
    // verify auth token
    var givenKey = request.header("Authorization");
    if (givenKey === undefined || givenHash === "") {
        console.log("Sending status 400. Missing auth token.");
        return response.status(400).send("Missing auth token.");
    }

    if (verifyKeyHash(functions.config().twitch.key, givenKey) === false && verifyKeyHash(functions.config().twitch.key2, givenKey) === false){
        return response.status(400).send("Invalid Auth token");
    }

    // verify channel given
    var channelId = request.query.channelId;
    if (channelId === undefined) {
        return response.status(400).send("Missing channel Id");
    }

    var trailRef = db.ref(`${twitchAuraRoute}/${channelId}`);

    return trailRef.set(true).then((snapshot) => {
        return response.sendStatus(200);
    }).catch((err) => {
        console.log(err);
        return response.sendStatus(500);
    });
});