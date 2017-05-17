'use strict';
require('dotenv').config();

// sample-powertrack-config.js - sample configuration file for cognicity-reports-powertrack module

/**
 * Configuration for cognicity-reports-powertrack
 * @namespace {object} config
 * @property {object} pg Postgres configuration object
 * @property {string} pg.table_invitees Postgres table name for invited user records
 * @property {object} gnip Configuration object for Gnip PowerTrack interface
 * @property {boolean} gnip.stream If true, connect to the Gnip stream and process tweets
 * @property {number} gnip.streamTimeout Network timeout for Gnip stream connection, in milliseconds. Must be >30s as a keep-alive is sent at least every 30s. {@link http://support.gnip.com/apis/consuming_streaming_data.html#keepalive_signals}
 * @property {string} gnip.username Username for Gnip PowerTrack
 * @property {string} gnip.password Password for Gnip PowerTrack
 * @property {string} gnip.streamUrl URL for Gnip PowerTrack stream, take from the PowerTrack admin interface. http://support.gnip.com/apis/consuming_streaming_data.html#Backfill}
 * @property {string} gnip.rulesUrl URL for the Gnip PowerTrack rules interface, take from the PowerTrack admin interface.
 * @property {object} gnip.rules Object of Gnip rules mapping rule names to rule text
 * @property {string} gnip.rules.(name) Rule name
 * @property {string} gnip.rules.(value) Rule text
 * @property {number} gnip.maxReconnectTimeout Maximum reconnection delay in milliseconds. Exponential backoff strategy is used starting at 1000 and will stop growing at this value.
 * @property {object} twitter Configuration object for Twitter interface
 * @property {object} twitter.usernameVerify Twitter username (without @) authorised to verify reports via retweet functionality
 * @property {string} twitter.usernameReplyBlacklist Twitter usernames (without @, comma separated for multiples) which will never be responded to as part of tweet processing
 * @property {string} twitter.consumer_key Take from the twitter dev admin interface
 * @property {string} twitter.consumer_secret Take from the twitter dev admin interface
 * @property {string} twitter.access_token_key Take from the twitter dev admin interface
 * @property {string} twitter.access_token_secret Take from the twitter dev admin interface
 * @property {boolen} twitter.send_enabled If true, send tweets to users asking them to verify their reports
 * @property {number} twitter.url_length Length that URLs in tweets are shortened to
 * @property {string} twitter.defaultLanguage The default language code to use if we can't resolve one from the tweet
 * @property {boolean} twitter.addTimestamp If true, append a timestamp to each sent tweet
 * @property {object} twitter.media_id Media to be included with auto-reply tweets
 */
var config = {};

//Database tables
config.pg = {};
config.pg.table_invitees = 'twitter.invitees';

// Gnip Powertrack API
config.gnip = {};
config.gnip.stream = true; // Connect to stream and log reports
config.gnip.streamTimeout = 1000 * 60; // In milliseconds. Must be >30s as a keep-alive is sent at least every 30s
config.gnip.username = process.env.GNIP_USERNAME; // Gnip username
config.gnip.password = process.env.GNIP_PASSWORD; // Gnip password
config.gnip.streamUrl = process.env.GNIP_STREAM_URL; // Gnip stream URL, take from the Gnip admin interface.
config.gnip.rulesUrl = process.env.GNIP_RULES_URL; // Gnip rules URL, take from the Gnip admin interface.
// Gnip rules, enter as an object where the key is the rule name and the value is the rule as a string
config.gnip.rules = {
    "boundingbox":"(contains:flood OR contains:rains) (bounding_box:[80.0900 12.8400 80.3800 13.0517] OR bounding_box:[80.0900 13.0517 80.3800 13.2555])",
    "addressed":"(contains:flood OR contains:rains) @riskmapindia",
    "location":"(contains:flood OR contains:rains) (bio_location:chennai OR place:chennai OR bounding_box:[80.0900 12.8400 80.3800 13.0517] OR bounding_box:[80.0900 13.0517 80.3800 13.2555])"
};

config.gnip.maxReconnectTimeout = 1000 * 60 * 5; // In milliseconds; 5 minutes for max reconnection timeout - will mean ~10 minutes from first disconnection
config.gnip.backfillMinutes = 5; // backfill in minutes on reconnect to the stream

// Twitter app authentication details
config.twitter = {};
//TODO grasp & re-tweet verification see #3
config.twitter.usernameVerify = ''; // Twitter username (without @) authorised to verify reports via retweet functionality
config.twitter.usernameReplyBlacklist = 'petabencana,BPBDJakarta'; // Twitter usernames (without @, comma separated for multiples) which will never be sent to in response to tweet processing
config.twitter.consumer_key = process.env.TWITTER_CONSUMER_KEY; // Take from the twitter dev admin interface
config.twitter.consumer_secret = process.env.TWITTER_CONSUMER_SECRET; // Take from the twitter dev admin interface
config.twitter.access_token_key = process.env.TWITTER_ACCESS_TOKEN_KEY; // Take from the twitter dev admin interface
config.twitter.access_token_secret = process.env.TWITTER_ACCESS_TOKEN_SECRET; // Take from the twitter dev admin interface

// Twitter parameters
config.twitter.send_enabled = true; // Enable sending of tweets?
config.twitter.url_length = 0; // URLs no longer count as part of tweet limits so this should be 0

// Twitter message texts
// Note we use IN and ID because twitter and Gnip return different language codes for Indonesian
// The messages should be no longer than 109 characters if timestamps are enabled, or 123 characters if timestamps are disabled
// TODO - ADD SUPPORT FOR PREP CARD.
config.twitter.defaultLanguage = 'en'; // The default language code to use if we can't resolve one from the tweet
// Dialogue containers
config.twitter.dialogue = {};
config.twitter.dialogue.ahoy = {};          // Greet users
config.twitter.dialogue.requests = {};      // Respond to user requests
config.twitter.dialogue.requests.card = {}  // Flood report card responses
// Dialogue translations
config.twitter.dialogue.ahoy.en = 'Hello, I am RiskMapBot, reply with #flood to send me your flood report.'
config.twitter.dialogue.ahoy.id =  'Halo, saya RiskMapBot. Untuk melaporkan banjir di sekitarmu, silakan balas dengan #banjir.'
config.twitter.dialogue.ahoy.in = 'Hello, I am RiskMapBot, reply with #flood to send me your flood report.'
// Respond to user requests
config.twitter.dialogue.requests = {};
config.twitter.dialogue.requests.card.en = 'Hi! Report flood using this link. Thanks!';
config.twitter.dialogue.requests.card.id = 'Hai! Gunakan link ini untuk menginput lokasi banjir, keterangan, & foto.';
config.twitter.dialogue.requests.card.in = 'Hi! Report flood using this link. Thanks!';





// Append a timestamp to each sent tweet except response to confirmed reports with unique urls
config.twitter.addTimestamp = false;

// Add a specified twitter media to replies
config.twitter.media_id = {};
// See note above - GNIP uses 'in' to represent the Indonesian language.
config.twitter.media_id.id = process.env.TWITTER_MEDIA_ID_ID;
config.twitter.media_id.en = process.env.TWITTER_MEDIA_ID_EN;

// Cognicity details
config.cognicity = {};
config.cognicity.server = process.env.COGNICITY_SERVER;
config.cognicity.card_url_prefix = process.env.CARDS_PREFIX;
config.cognicity.network = 'twitter';
config.cognicity.x_api_key = process.env.X_API_KEY;

// Export config object
module.exports = config;
