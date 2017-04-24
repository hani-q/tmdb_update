'use strict';

const functions = require('firebase-functions');
const admin = require('firebase-admin');
var defaultApp = admin.initializeApp(functions.config().firebase);
var defaultDatabase = defaultApp.database();
const secureCompare = require('secure-compare');
const page_width = 250
// Imports the Google Cloud client library
const PubSub = require('@google-cloud/pubsub');

global.data = [];

// Your Google Cloud Platform project ID
const projectId = 'anime-db-bab96';

// Instantiates a client
const pubsub = PubSub({
  projectId: projectId
});

// The name for the new topic
const topicName = 'tmdb';

var Client = require('node-rest-client').Client;
var client = new Client();
// API key
//TODO(Hani Q) need to fetch this from Remote Config
const key = '2410f30eccdd2c2aa45c44dd6c5603a9';
const base_url = 'http://api.themoviedb.org/3/discover/movie?language=en-US&with_genres=16,12%7C35%7C10751&sort_by=popularity.desc&include_adult=false&api_key=' + key + '&page=';
var sleep = require('sleep');
/**
 * When requested this Function will just public a message on 'tmdb' topic.
 * The request needs to be authorized by passing a 'key' query parameter in the URL. This key must
 * match a key set as an environment variable using `firebase functions:config:set cron.key="YOUR_KEY"`.
 */
exports.tmdbUpdate = functions.https.onRequest((req, res) => {
    const key = req.query.key;
    // Exit if the keys don't match
    if (!secureCompare(key, functions.config().cron.key)) {
        console.log('The key provided in the request does not match the key set in the environment. Check that', key,
                    'matches the cron.key attribute in `firebase env:get`');

        res.status(403).send('Security key does not match. Make sure your "key" URL query parameter matches the ' +
                             'cron.key environment variable.');
        return;
    }


    // Create the topic if it doesnt exist
    pubsub.createTopic(topicName, (err, topic) => {
        // topic already exists.
        if (err && err.code === 409) {
            console.log(`Topic Already Exists.`);
            return;
        }
        console.log(`Topic ${topic.name} created.`);
        return;
    });

    // Publish 'start' message on topic for update function to start
    publishMessage(topicName, "start");
    res.send("Db Update Started in Background")
});

/**
 * When requested this Function will just public a message on 'tmdb' topic.
 * The request needs to be authorized by passing a 'key' query parameter in the URL. This key must
 * match a key set as an environment variable using `firebase functions:config:set cron.key="YOUR_KEY"`.
 */
exports.tmdbUpdatePubSub = functions.pubsub.topic('tmdb').onPublish(event => {
    const pubSubMessage = event.data;
    // Decode the PubSub Message body.
    const messageBody = pubSubMessage.data ? Buffer.from(pubSubMessage.data, 'base64').toString() : null;

    // Print the message in the logs.
    console.log(`Message recieved ${messageBody || ''}`);

    // //console.log('Emptying DB');
    // var moviesRef = defaultDatabase.ref("v1/movies");
    // moviesRef.remove();
    // moviesRef.off();

    if (messageBody.localeCompare('start') != 0) {
        console.error.message("message should be start");
        return 409;
    }

    // // Get Total Pages
    var url = base_url + 1;;

    client.get(url, function (data, response) {

        console.log('Total Pages ' + data.total_pages);
        console.log('Started fetching Movies');
        // Calling fucntion to get datail


        var last_end = 1;
        var end = 1;

        for(var i = 1; i <= data.total_pages; ) {
            if(i + page_width > data.total_pages)
                end = data.total_pages;
            else
                end = i + page_width - 1;

            console.log(`Fetching from ${i} - ${end}`);
            get_data(i, end, results);

            i = i + page_width;
            // last_end = end;
        }
        return;
    });
    return;
});


/**
 * When called this will publish the message on a specific topic
 */
function publishMessage (topicName, data) {

  const topic = pubsub.topic(topicName);

  // Publishes the message
  return topic.publish(data)
    .then((results) => {
        const messageIds = results[0];
        console.log(`Message ${messageIds[0]} published.`);
        return messageIds;
    });
}

/**
 * When called this will call the Movie DB API with Auth Key.
 */
function get_data (page, end, callback) {
    var job = page;
    var url = base_url + page;

    client.get(url, function (data, response) {

        // Get rateLimit from Raw Header
        var ratelimit = response.rawHeaders[27];

        // console.log(response.rawHeaders[26] + ' ' + response.rawHeaders[27]);
        if (ratelimit == 2) {
            // Rate Limit Exceeded wait 10 seconds
           console.log("Rate Limited Exceeded");

           // Sleep 10 Seconds to reset Rate Limit
           sleep.sleep(10);
        }

        console.log('Page ' + page + '/' + end);

        global.data.push(data.results);
        page = page + 1;
        if (page <= end) {
            sleep.sleep(2);
            get_data(page, end, results);
            return;
        }
        callback(job);
        return;
    });
    return;
}

function results (page) {
    console.log('Total Pages so far = ' + Object.keys(global.data).length);
    var moviesRef = defaultDatabase.ref("v1/movies");
    //moviesRef.remove();
    var data_json = {};
    for (var i = 0; i < global.data.length; i++) {
        for (var j = 0; j < global.data[i].length; j ++) {
            var res_array = global.data[i];
            var toon = res_array[j];
            data_json[toon.id] = toon;
        }
        break;
    }
    moviesRef.update(data_json);
    moviesRef.off();
    console.log("finished fetching movies for job" + page);
    return;
}
