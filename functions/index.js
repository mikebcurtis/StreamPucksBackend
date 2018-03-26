const admin = require('firebase-admin');
const functions = require('firebase-functions');
const jwt = require('jsonwebtoken');
//const cors = require('cors')({origin: true});
const jwtHeaderName = "x-extension-jwt";
const collectionName = "twitchplaysballgame";
const launchesRoot = "launches";
const request = require('request-promise');

admin.initializeApp(functions.config().firebase);

var db = admin.database();

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
    var token = request.get(jwtHeaderName);
    if (token === undefined || token.length <= 0){
        console.log("Sending status 401. JWT is missing."); // DEBUG
        return response.sendStatus(401); // request is missing token, unauthorized
    }

    var encodedKey = functions.config().twitch.key;
    if (encodedKey === undefined) {
        console.log("Sending status 500. Could not find twitch key."); // DEBUG
        return response.sendStatus(500); // can't find twitch key, internal error
    }

    var key = Buffer.from(encodedKey, 'base64');
    try {
        var verification = jwt.verify(token, key);
    }
    catch(err) {
        console.log("Sending status 401. Could not verify JWT."); // DEBUG
        return response.sendStatus(401); // provided token was incorrect, unauthorized
    }

    var tokenVerifyTime = Date.now(); // DEBUG
    console.log("Verify token took: " + (tokenVerifyTime - start)); // DEBUG

    // verify json is correct
    var launchData = request.body;
    console.log("request body: "); // DEBUG
    console.log(request.body); // DEBUG

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

    // update database
    // first, generate a promise for each launch object
    var launchPromises = [];

    for(i = 0; i < launchData.length; i++) {
        var launchKey = launchData[i].id;
        var newLaunch = {};
        newLaunch['/' + launchesRoot + '/' + launchKey] = launchData[i];
        launchPromise = db.ref().update(newLaunch).catch(reason => {
            console.log(reason);
            return response.sendStatus(500);
        });
        launchPromises.push(launchPromise);
    }

    var generateKeyTime = Date.now(); // DEBUG
    console.log("Generating promises took: " + (generateKeyTime - verifyJsonTime)); // DEBUG

    return Promise.all(launchPromises).then((snapshot) => {
        console.log("Executing promises took: " + (Date.now() - generateKeyTime));
        return response.sendStatus(200);
    });
});

exports.puckUpdate = functions.database.ref('/players/{channelId}/{opaqueUserId}')
    .onUpdate(event => {
        //TODO verify JWT
        var encodedKey = functions.config().twitch.key;
        if (encodedKey === undefined) {
            console.log("Sending status 500. Could not find twitch key."); // DEBUG
            return response.sendStatus(500); // can't find twitch key, internal error
        }
        var token = {
            "exp": Date.now() + 60,
            "channel_id": channelId,
            "pubsub_perms": {
                send: ["*"]
            }
        };
        var signedToken = jwt.sign(token, encodedKey);
        var options = {
            method: 'POST',
            uri: 'https://api.twitch.tv/extensions/message/' + channelId,
            auth: {
                'bearer': signedToken
            },
            body: {
                "content_type": "application/json",
                "message": JSON.stringify({
                    'puckCount': event.data.val().puckCount
                }),
                "targets": ["whisper-" + opaqueUserId]
            },
            json: true // Automatically stringifies the body to JSON
        };

        rp(options)
            .then(function (parsedBody) {
                response.send(200);
            })
            .catch(function (err) {
                response.send(500);
            });
    });


