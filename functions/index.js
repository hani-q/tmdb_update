'use strict';

const date = new Date();

const functions = require('firebase-functions');
const admin = require('firebase-admin');
const async = require("async");
const Client = require('node-rest-client').Client;
const sleep = require('sleep');
const secureCompare = require('secure-compare');

// Imports the Google Cloud client library
const PubSub = require('@google-cloud/pubsub');
// Instantiates a pubsub client
const pubsub = PubSub({projectId: 'anime-db-bab96'});

var defaultApp = admin.initializeApp(functions.config().firebase);
var defaultDatabase = defaultApp.database();

global.all_data = [];
global.data_info = {};
global.failed_pages = [];

// The name for the new topic
const topicName = 'tmdb';

// Initiate rest Client
var client = new Client();

// Read API key from firbase env config
const key = functions.config().tmdb.key;
const base_url = 'http://api.themoviedb.org/3/discover/movie?language=en-US&with_genres=16,12%7C35%7C10751&sort_by=popularity.desc&include_adult=false&api_key=' + key + '&page=';

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

    var url = base_url + 1;
    console.log("Getting Data from [" + url + "]");
    client.get(url, (data, response) => {
        // Publish 'start' message on topic for update function to start
        var total_pages = data.total_pages;
        var total_results = data.total_results;
        console.log("Sending message to fetch Pages=" + total_pages + " Results=" + total_results);
        publishMessage(topicName, {"total_pages": total_pages, "total_results": total_results});
    }).on('error', (err) => {
        console.log('something went wrong on the request, please restart', err.request.options);
    });

    res.send("DB update started in Background")
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
    console.log(`Message recieved ${messageBody || ''}`);

    // //console.log('Emptying DB');
    // var moviesRef = defaultDatabase.ref("v1/movies");
    // moviesRef.remove();
    // moviesRef.off();

    // Array to hold async tasks
    var asyncTasks = [];

    var page = 0;
    global.data_info = JSON.parse(messageBody);
    var total_pages = global.data_info.total_pages;

    var total_results = global.data_info.total_results;
    var problem_pages = [];

    console.log('Started fetching Movies');
    console.log('Total Pages ' + total_pages);

    // Loop total no of pages times
    for (var i = 0; i < total_pages; i++) {
        // We don't actually execute the async action here
        // We add a function containing it to an array of "tasks"
        asyncTasks.push((callback) => {
            page++;

            //Adding a random sleep to each call
            //var sleep_int = getRandomInt(1,6);
            //var sleep_int = "N/A";
            console.log("Processing Page " + page);
            //sleep.sleep(sleep_int);

            var url = base_url + page;
            client.get(url, (data, response) => {

                // Get rateLimit from Raw Header
                var ratelimit = response.rawHeaders[27];

                // console.log(response.rawHeaders[26] + ' ' + response.rawHeaders[27]);
                if (ratelimit == 2) {
                    // Rate Limit Exceeded wait 10 seconds
                   console.log("Rate Limited Exceeded");

                   // Sleep 10 Seconds to reset Rate Limit
                   sleep.sleep(10);
                }

                global.all_data = global.all_data.concat(data.results);
                callback();
            }).on('error', (err) => {
                // Retrying once more before failure
                var eurl = err.request.options.href;
                console.log('Something went wrong on the request', err.request.options);
                problem_pages.push(eurl);
                sleep.sleep(2);
                client.get(eurl, (data, response) => {
                    console.log("Retrying url " + eurl);
                    global.all_data = global.all_data.concat(data.results);
                    callback();
                }).on('error', (err) => {
                    global.all_data = global.all_data.concat(data.results);
                    var queryData = url.parse(eurl, true).query;
                    global.failed_pages.push(query.page);
                    console.log("Permanent Failure on Page " + page);
                    var date_string = date.toISOString();
                    var datim = Math.round(date/1000);
                    var statusRef = defaultDatabase.ref("v1/status");
                    statusRef.update({
                                        "status": "failed",
                                        "timestamp": date_string,
                                        "failed_pages": failed_pages,
                                        "records_fetched": 0,
                                        "records_expected": global.data_info.total_results,
                                        "total_pages": global.data_info.total_page
                                    });
                    statusRef.off();
                    throw new Error("Perma Fail on " + query.page);
                });
            });
        });
    }

    //Execute the TaskArray in parallel
    const limit = functions.config().exec.limit;
    console.log("Starting || execution [limit " + limit + "]");
    async.parallelLimit(asyncTasks, limit, movie_results);
});



function getRandomInt(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min;
}

/**
 * When called this will publish the message on a specific topic
 */
function publishMessage (topicName, data) {
    const topic = pubsub.topic(topicName);

    // Publishes the message
    return topic.publish(data).then((results) => {
        const messageIds = results[0];
        console.log(`Message ${messageIds[0]} published.`);
        return messageIds;
    });
}


function movie_results () {
    var moviesRef = defaultDatabase.ref("v1/movies");
    ;

    //moviesRef.remove();
    var data_json = {};

    console.log("Total Movies fetched " + global.all_data.length);

    async.each(global.all_data,
        (mov, cb) => {
            data_json[mov.id] = mov;
            cb();
            },
        () => {
            var data_len = Object.keys(data_json).length;
            console.log("Result post-processing done");
            console.log("Total = " + data_len + " | Expected = " + global.data_info.total_results);
            console.log("Uploading to Firebase Datastore");
            moviesRef.update(data_json);

            console.log("Finished fetching movies");

            // Update status for this job on status DB
            console.log("Updating Status");
            var date_string = date.toISOString();
            var datim = Math.round(date/1000);
            var statusRef = defaultDatabase.ref("v1/status");
            statusRef.update({
                                "status": "passed",
                                "records_fetched": data_len,
                                "records_expected": global.data_info.total_results,
                                "total_pages": global.data_info.total_pages,
                                "timestamp": date_string,
                                "failed_pages": failed_pages
                            });

            // Closing all db references
            console.log("Closing DB handles");
            moviesRef.off();
            statusRef.off();
            console.log("All Done");
        }
    );
}



