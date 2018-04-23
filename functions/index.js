const admin = require('firebase-admin');
const functions = require('firebase-functions');
const jwt = require('jsonwebtoken');
const request = require('request');
const rp = require('request-promise');
const jwtHeaderName = "x-extension-jwt";
const collectionName = "twitchplaysballgame";
const launchesRoot = "launches";
const playersRoot = "players";

admin.initializeApp(functions.config().firebase);
var db = admin.database();

var verifyJwt = function(token) {
    if (token === undefined) {
        return [false, 401, "Missing signed JWT."];
    }

    var encodedKey = functions.config().twitch.key;

    if (encodedKey === undefined) {
        return [false, 500, "Internal error."];
    }

    var key = Buffer.from(encodedKey, 'base64');
    try {
        var verification = jwt.verify(token, key);
    }
    catch(err) {
        console.log("Sending status 401. Could not verify JWT."); // DEBUG
        return [false, 401, "Sending status 401. Could not verify JWT"];
    }

    return [true];
};

exports.queueLaunch = functions.https.onRequest((request, response) => {
    var start = Date.now();

    if (request.method === 'OPTIONS') {
        console.log("Sending status 200. CORS check successful."); // DEBUG
        console.log("CORS return elapsed time: " + (Date.now() - start)); // DEBUG
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

    var tokenVerifyTime = Date.now(); // DEBUG
    console.log("Verify token took: " + (tokenVerifyTime - start)); // DEBUG

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

    var verifyJsonTime = Date.now(); // DEBUG
    console.log("Verify JSON took: " + (verifyJsonTime - tokenVerifyTime)); // DEBUG

    // update database, excluding launches that have 0 pucks or are undefined
    var launchPromises = [];
    for(i = 0; i < launchData.length; i++) {
        if (launchData[i].pucks !== undefined && launchData[i].pucks <= 0) {
            continue;
        }
        //var newLaunch = {};
        var ref = db.ref(`${launchesRoot}/${channelId.trim()}`);
        //var localOpaqueId = launchData[i].opaqueUserId;
        //var localPlayerId = launchData[i].userId;
        //var playerRef = db.ref('{playersRoot}/{channelId}/' + localPlayerId);
        //playerRef.push(localOpaqueId);
        //var newChildRef = ref.child(launchData[i]); //creates a ref to the child with the name of the ID
        //newLaunch[launchData[i].id] = launchData[i];
        launchPromises.push(
            ref.push().set(launchData[i]).catch(reason => {
                console.log(reason);
                return response.sendStatus(500);
            })
        );
    }

    var generateKeyTime = Date.now(); // DEBUG
    console.log("Generating promises took: " + (generateKeyTime - verifyJsonTime)); // DEBUG

    if (launchPromises.length <= 0) {
        return response.sendStatus(200);
    }

    return Promise.all(launchPromises).then((snapshot) => {
        console.log("Executing promises took: " + (Date.now() - generateKeyTime));
        return response.sendStatus(200);
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
    return channelRef.once('value').then(snapshot => {
        var promise;
        if (!snapshot.hasChild(playerId)) {
            var playerRef = channelRef.child(playerId);
            return playerRef.set({
                                        'points': 0,
                                        'puckCount': 30,
                                        'opaqueUserId': opaqueUserId,
                                        'lastSeen': Date.now()
                                    });
        }
        else {
            playerRef = channelRef.child(playerId);
            return playerRef.update({ 'lastSeen': Date.now() });
        }
    }).then(snapshot => {
        return response.sendStatus(200);
    }).catch(reason => {
        console.log(reason);
        return response.sendStatus(500);
    });
    // update the last seen timestamp
    //db.ref(`${playersRoot}/${channelId}/${playerId}`).update({ 'lastSeen': Date.now() }).then(snapshot => {
    //    return response.sendStatus(200);
    //}).catch(reason => {
    //    console.log(reason);
    //    return response.sendStatus(500);
    //});
});

exports.puckUpdate = functions.database.ref('{playersRoot}/{channelId}/{playerId}/puckCount').onWrite(event => {
    // generate and sign JWT
    var encodedKey = functions.config().twitch.key;
    var clientId = functions.config().twitch.id;
    if (encodedKey === undefined || clientId === undefined) {
        console.log("Sending status 500. Could not find twitch key or client ID"); // DEBUG
        return; // can't find twitch key, internal error
    }
    var token = {
        "exp": Date.now() + 60,
        "user_id": event.params.playerId.trim(),
        "role":"external",
        "channel_id": event.params.channelId.trim(),
        "pubsub_perms": {
            send: ["*"]
        }
    };
    var signedToken = jwt.sign(token, Buffer.from(encodedKey, 'base64'), { 'noTimestamp': true });
    var opaqueRef = db.ref(`${playersRoot}/${event.params.channelId}/${event.params.playerId}/opaqueUserId`);
    var opaqueUserId;
    return opaqueRef.once('value').then(snapshot => {
        opaqueUserId = snapshot.val();
        console.log(snapshot.val());

        if (opaqueUserId === undefined) {
            console.log("opaque user id was undefined");
            return;
        }
        //return response.sendStatus(200);
        var target = "whisper-" + opaqueUserId;
        console.log(target);
    
        // send PubSub message
        var options = {
            method: 'POST',
            uri: 'https://api.twitch.tv/extensions/message/' + event.params.channelId.trim(),
            auth: {
                'bearer': signedToken
            },
            headers: {
                "Client-ID": clientId
            },
            body: {
                "content_type": "application/json",
                "message": JSON.stringify({
                    "puckCount": event.data.val()
                }),
                "targets": [target]
                //"targets": ["broadcast"]
            },
            json: true // Automatically stringifies the body to JSON
        };
        
        return rp(options);
    }).catch(reason => {
        console.log(reason);
    });
});