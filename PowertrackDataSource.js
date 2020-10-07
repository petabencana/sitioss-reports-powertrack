'use strict';

// Request module to call cognicity-server
const request = require('request');
require('dotenv').config({silent:true});

// Prototype object this object extends from - contains basic twitter interaction functions
var BaseTwitterDataSource = require('../BaseTwitterDataSource/BaseTwitterDataSource.js');

/**
 * The Gnip Powertrack data source.
 * Connect to the Gnip Powertrack stream and process matching tweet data.
 * @constructor
 * @augments BaseTwitterDataSource
 * @param {Reports} reports An instance of the reports object.
 * @param {object} twitter Configured instance of twitter object from ntwitter module
 * @param {object} config Gnip powertrack specific configuration.
 */
var PowertrackDataSource = function PowertrackDataSource(
		reports,
		twitter,
		config
	){

	// Store references to constructor arguments
	this.config = config;

	BaseTwitterDataSource.call(this, reports, twitter);

	// Gnip PowerTrack interface module
	this.Gnip = require('gnip');

	this.reports = reports;

	this.lastTweetID = 0;

	// Set constructor reference (used to print the name of this data source)
	this.constructor = PowertrackDataSource;
};

// Set our prototype to be the base object
PowertrackDataSource.prototype = Object.create( BaseTwitterDataSource.prototype );

/**
 * Instance of Gnip object from Gnip module
 * @type {object}
 */
PowertrackDataSource.prototype.Gnip =  null;

/**
 * Data source configuration.
 * This contains the data source specific configuration.
 * @type {object}
 */
PowertrackDataSource.prototype.config = {};

/**
 * Check new tweet ID is greater than the current one
 @param {GnipTweetActivity} tweetActivity Tweet activity object (i.e. new tweet)
 @param {function} callback Function to call if test successful
 */

PowertrackDataSource.prototype._checkAgainstLastTweetID = function(tweetActivity, callback){

	var self = this;

	var tweet_id = self._parseTweetIdFromActivity(tweetActivity);

	if (tweet_id > self.lastTweetID){
		callback(tweetActivity);
	}
};

/**
 * Store the last seen tweet ID and then call the tweet processor.
 @param {GnipTweetActivity} tweetActivity Tweet activity object
 @param {function} callback function to call once the last tweet once the ID has been stored
 */

PowertrackDataSource.prototype._storeTweetID = function(tweetActivity, callback) {
 var self = this;

 var tweet_id = self._parseTweetIdFromActivity(tweetActivity);

 self.reports.dbQuery(
	 {
		 text: "UPDATE twitter.seen_tweet_id SET id=$1;",
		 values: [tweet_id]
	 },
	 function(result) {
			 self.logger.verbose('Recorded tweet ' + tweet_id + ' as having been seen.');
			 callback();
		 }
 );
};

/**
 * Retrieve and set the last seen tweetID
 @param {function} callback function to call once the last seen tweet ID has been loaded.
 */

PowertrackDataSource.prototype._getlastTweetIDFromDatabase = function(callback) {
	var self = this;
	self.reports.dbQuery(
		{
			text: "SELECT id FROM twitter.seen_tweet_id;"
		},
		function(result) {
			if(result.rows && result.rows.length > 1) {
				self.lastTweetID = Number(result.rows[0].id);
				callback();
			} else {
				self.lastTweetID = 0;
				callback();
			}
		}
	);
};

/**
 * Gnip PowerTrack Tweet Activity object.
 * @see {@link http://support.gnip.com/sources/twitter/data_format.html}
 * @typedef GnipTweetActivity
 */

/**
 * Main stream tweet filtering logic.
 * Filter the incoming tweet and decide what action needs to be taken:
 * confirmed report, ask for geo, ask user to participate, or nothing
 * @param {GnipTweetActivity} tweetActivity The tweet activity from Gnip
 */
PowertrackDataSource.prototype.filter = function(tweetActivity) {
	var self = this;
	self.logger.verbose( 'filter: Received tweetActivity: screen_name="' + tweetActivity.user.screen_name + '", text="' + tweetActivity.body.replace("\n", "") + '", coordinates="' + (tweetActivity.geo && tweetActivity.geo.coordinates ? tweetActivity.geo.coordinates[1]+", "+tweetActivity.geo.coordinates[0] : 'N/A') + '"' );

	function botTweet(err, message) {
		if (err){
			self.logger.error('Error calling parseRequest - no reply sent');
		}
		else {
			// tweetActivity, null media, message, null callback
			self._sendReplyTweet(tweetActivity, null, message, null);
		}
	}

	function botTweetWithMedia(err, message) {
		if (err) {
			self.logger.error('Error calling parseRequest - no reply sent');
		}
		else {
			// Set default media link
			var media = self.config.twitter.media_id.ph;
			// Get language of user's tweet
			var lang = self._parseLangsFromActivity(tweetActivity)[0];
			// Switch media to English if required
			if (lang === 'en'){
				media = self.config.twitter.media_id.en;
			}
			// Send tweet
			// tweetActivity, media, message, null callback
			self._sendReplyTweet(tweetActivity, media, message, null);
		}
	}

	function parseRequest(tweetActivity, addressed){
		var username = tweetActivity.user.screen_name;
		var words = tweetActivity.body;
    var filter = words.match(/baha|flood/gi);
	var language = self._parseLangsFromActivity(tweetActivity)[0];
	var disaster = 'default';
    if (filter){filter = filter[0];}

    switch (filter){
      case null:
        self.logger.info('Bot could not detect request keyword');
		// self._ahoy(username, 'default' ,language, botTweet); //Respond with default
		return;

      case 'baha':
		self.logger.info('Bot detected request keyword "baha"');
		language = 'ph';
		disaster = 'flood';
        // self._getCardLink(username, self.config.twitter.network_name, language, 'flood', botTweet);
		break;

      case 'flood':
		self.logger.info('Bot detected request keyword "flood"');
		language = 'en';
		disaster = 'flood';
		// self._getCardLink(username, self.config.twitter.network_name, language, 'flood', botTweet);
		break;

      case 'gempa':
		self.logger.info('Bot detected request keyword "gempa"');
		language = 'ph';
		disaster = 'earthquake';
        // self._getCardLink(username, self.config.twitter.network_name, language, 'earthquake', botTweet);
		break;

      case 'earthquake':
		self.logger.info('Bot detected request keyword "earthquake"');
		language = 'en';
		disaster = 'earthquake';
		// self._getCardLink(username, self.config.twitter.network_name, language, 'earthquake', botTweet);
		break;
	  case 'forestfire':
		self.logger.info('Bot detected request keyword "forestfire"');
		language = 'en';
		disaster = 'fire';
		break;
	  case 'kebakaranhutan':
		self.logger.info('Bot detected request keyword "kebakaranhutan"');
		language = 'ph';
		disaster = 'fire';
		break;
	  case 'haze':
		self.logger.info('Bot detected request keyword "haze"');
		language = 'en';
		disaster = 'haze';
		break;
	  case 'kabutasap':
		self.logger.info('Bot detected request keyword "kabutasap"');
		language = 'ph';
		disaster = 'haze';
		break;
	  case 'volcano':
		self.logger.info('Bot detected request keyword "volcano"');
		language = 'en';
		disaster = 'volcano';
		break;
	  case 'gunungapi':
		self.logger.info('Bot detected request keyword "gunungapi"');
		language = 'ph';
		disaster = 'volcano';
		break;
	  case 'extremewind':
		self.logger.info('Bot detected request keyword "extremewind"');
		language = 'en';
		disaster = 'wind';
		break;
	  case 'anginkencang':
		self.logger.info('Bot detected request keyword "anginkencang"');
		language = 'ph';
		disaster = 'wind';
		break;

	  case 'prep':
		self.logger.info('Bot detected request keyword "prep"');
		disaster = 'prep';
		// self._getCardLink(username, self.config.twitter.network_name, language, 'prep', botTweet);
		break;
	}
	if (addressed)
		self._getCardLink(username, self.config.twitter.network_name, language, disaster, botTweet);
	else
		_sendStart(username, language, disaster, tweetActivity)
	
	}

	function _sendStart(username, language, disaster, tweetActivity) {
		self._ifNewUser(tweetActivity.user.screen_name, function(username_hash){
			self._ahoy(username, disaster, language, botTweetWithMedia); //Respond with default
			self._insertInvitee(tweetActivity);
		});
		return;
	}

	function sendAhoy(tweetActivity){
		var username = tweetActivity.user.screen_name;
		var language = self._parseLangsFromActivity(tweetActivity)[0];

		self._ifNewUser(tweetActivity.user.screen_name, function(username_hash){
			self._ahoy(username, language, botTweetWithMedia); //Respond with default
			self._insertInvitee(tweetActivity);
		});
		return;
	}

	// Everything incoming has a keyword already, so we now try and categorize it using the Gnip tags
	var insideArea = false;
	var addressed = false;

	tweetActivity.gnip.matching_rules.forEach( function(rule){
		if (rule.tag) {
			if (rule.tag.indexOf("addressed")===0) addressed = true;
			self.config.gnip.areaTags.forEach( function(areaTag){
				if (rule.tag.indexOf(areaTag)===0) insideArea = true;
			});
		}
	});

	// Perform the actions for the categorization of the tweet
	if ( insideArea && addressed ) {

		self.logger.verbose("Tweet is addressed and within target city -> parse by bot");

		parseRequest(tweetActivity, addressed);

	} else if ( insideArea && !addressed ) {

		self.logger.verbose("Tweet is not addressed and within target city -> send ahoy");

		parseRequest(tweetActivity, addressed);

	} else if ( !insideArea && addressed ) {

		self.logger.verbose("Not in target city but addressed -> parse by bot");

		parseRequest(tweetActivity, addressed);

	} else {
		// Not in bounding box but has geocoordinates or no location match
		self.logger.warn( 'filter: Tweet did not match category actions (not in target city nor addressed)' );
	}

};

/**
 * Connect the Gnip stream.
 * Establish the network connection, push rules to Gnip.
 * Setup error handlers and timeout handler.
 * Handle events from the stream on incoming data.
 */
PowertrackDataSource.prototype.start = function() {
	var self = this;

	// 1. Get last seen tweet from the database, store locally on startup.
	// 2. For every tweet store in database even if doesn't pass filter.

	// Gnip stream
	var stream;
	// Timeout reconnection delay, used for exponential backoff
	var _initialStreamReconnectTimeout = 1000;
	var streamReconnectTimeout = _initialStreamReconnectTimeout;
	// Connect Gnip stream and setup event handlers
	var reconnectTimeoutHandle;
	// Send a notification on an extended disconnection
	var disconnectionNotificationSent = false;

	// Attempt to reconnect the socket.
	// If we fail, wait an increasing amount of time before we try again.
	function reconnectSocket() {
		// Try and destroy the existing socket, if it existsconfirmReports
		self.logger.warn( 'connectStream: Connection lost, destroying socket' );
		if ( stream._req ) stream._req.destroy();

		// If our timeout is above the max threshold, cap it and send a notification tweet
		if (streamReconnectTimeout >= self.config.gnip.maxReconnectTimeout) {
			// Only send the notification once per disconnection
			if (!disconnectionNotificationSent) {
				var message = "Cognicity Reports PowerTrack Gnip connection has been offline for " +
					self.config.gnip.maxReconnectTimeout + " seconds";
				self.reports.tweetAdmin(message);
				disconnectionNotificationSent = true;
			}
		} else {
			streamReconnectTimeout *= 2;
			if (streamReconnectTimeout >= self.config.gnip.maxReconnectTimeout) streamReconnectTimeout = self.config.gnip.maxReconnectTimeout;
		}

		// Attempt to reconnect
		self.logger.info( 'connectStream: Attempting to reconnect stream' );
		self._getlastTweetIDFromDatabase(function(){
			stream.start();
		});
	}

	// TODO We get called twice for disconnect, once from error once from end
	// Is this normal? Can we only use one event? Or is it possible to get only
	// one of those handlers called under some error situations.

	// Attempt to reconnect the Gnip stream.
	// This function handles us getting called multiple times from different error handlers.
	function reconnectStream() {
		if (reconnectTimeoutHandle) clearTimeout(reconnectTimeoutHandle);
		self.logger.info( 'connectStream: queing reconnect for ' + streamReconnectTimeout );
		reconnectTimeoutHandle = setTimeout( reconnectSocket, streamReconnectTimeout );
	}

	// Configure a Gnip stream with connection details
	stream = new self.Gnip.Stream({
	    url : self.config.gnip.streamUrl,
	    user : self.config.gnip.username,
	    password : self.config.gnip.password,
			backfillMinutes : self.config.gnip.backfillMinutes
	});

	// When stream is connected, setup the stream timeout handler
	stream.on('ready', function() {
		self.logger.info('connectStream: Stream ready!');
	    streamReconnectTimeout = _initialStreamReconnectTimeout;
	    disconnectionNotificationSent = false;
		// Augment Gnip.Stream._req (Socket) object with a timeout handler.
		// We are accessing a private member here so updates to gnip could break this,
	    // but gnip module does not expose the socket or methods to handle timeout.
		stream._req.setTimeout( self.config.gnip.streamTimeout, function() {
			self.logger.error('connectStream: Timeout error on Gnip stream');
			reconnectStream();
		});
	});

	// When we receive a tweetActivity from the Gnip stream this event handler will be called
	stream.on('tweet', function(tweetActivity) {
		// TODO - validate cache mode with new tweet id storage
		if (self._cacheMode) {
			self.logger.debug( "connectStream: caching incoming tweet for later processing (id=" + tweetActivity.id + ")" );
			self._cachedData.push( tweetActivity );
		} else {
			self.logger.debug("connectStream: stream.on('tweet'): tweet = " + JSON.stringify(tweetActivity));

			// Catch errors here, otherwise error in filter method is caught as stream error
			try {
				if (tweetActivity.user.screen_name) {
					// This looks like a tweet in Gnip activity format, store ID, then check for filter
					self._storeTweetID(tweetActivity, function(){
						self._checkAgainstLastTweetID(tweetActivity, function(tweetActivity){
							self.filter(tweetActivity);
						});
					});
				} else {
					// This looks like a system message
					self.logger.info("connectStream: Received system message: " + JSON.stringify(tweetActivity));
				}
			} catch (err) {
				self.logger.error("connectStream: stream.on('tweet'): Error on handler:" + err.message + ", " + err.stack);
			}
		}
	});

	// Handle an error from the stream
	stream.on('error', function(err) {
		self.logger.error("connectStream: Error connecting stream:" + err);
		reconnectStream();
	});

	// TODO Do we need to catch the 'end' event?
	// Handle a socket 'end' event from the stream
	stream.on('end', function() {
		self.logger.error("connectStream: Stream ended");
		reconnectStream();
	});

	// Construct a Gnip rules connection
	var rules = new self.Gnip.Rules({
	    url : self.config.gnip.rulesUrl,
	    user : self.config.gnip.username,
	    password : self.config.gnip.password
	});

	// Create rules programatically from config
	// Use key of rule entry as the tag, and value as the rule string
	var newRules = [];
	for (var tag in self.config.gnip.rules) {
		if ( self.config.gnip.rules.hasOwnProperty(tag) ) {
			newRules.push({
				tag: tag,
				value: self.config.gnip.rules[tag]
			});
		}
	}
	self.logger.debug('connectStream: Rules = ' + JSON.stringify(newRules));

	// Push the parsed rules to Gnip
	self.logger.info('connectStream: Updating rules...');
	// Bypass the cache, remove all the rules and send them all again
	rules.live.update(newRules, function(err) {
	    if (err) throw err;
		self.logger.info('connectStream: Connecting stream...');
		// If we pushed the rules successfully, get last seen report, and then try and connect the stream
		self._getlastTweetIDFromDatabase(function(){
			stream.start();
		});
	});
};

/**
 * Insert an invitee - i.e. a user we've invited to participate.
 * @param {GnipTweetActivity} tweetActivity Gnip PowerTrack tweet activity object
 */
PowertrackDataSource.prototype._insertInvitee = function(tweetActivity) {
	var self = this;

	self._baseInsertInvitee(tweetActivity.user.screen_name);
};

/**
 * Send @reply Twitter message
 * @param {GnipTweetActivity} tweetActivity The Gnip tweet activity object this is a reply to
 * @param {string} media_id The media_id of twitter media to embedd in tweet
 * @param {string} message The tweet text to send
 * @param {function} success Callback function called on success
 */
PowertrackDataSource.prototype._sendReplyTweet = function(tweetActivity, media_id, message, success) {
	var self = this;

	self._baseSendReplyTweet(
		tweetActivity.user.screen_name,
		self._parseTweetIdFromActivity(tweetActivity),
		media_id,
		message,
		success
	);
};

/**
 * Get tweet ID from Gnip tweet activity.
 * @param {GnipTweetActivity} tweetActivity The Gnip tweet activity object to fetch ID from
 * @return {string} Tweet ID
 */
PowertrackDataSource.prototype._parseTweetIdFromActivity = function(tweetActivity) {
	return tweetActivity.id.split(':')[2];
};

/**
 * Get retweet's original tweet ID from Gnip tweet activity.
 * @param {GnipTweetActivity} tweetActivity The Gnip tweet activity object to fetch retweet's original tweet ID from
 * @return {string} Tweet ID
 */
PowertrackDataSource.prototype._parseRetweetOriginalTweetIdFromActivity = function(tweetActivity) {
	return tweetActivity.object.id.split(':')[2];
};

/**
 * Get language codes from the activity.
 * @param {GnipTweetActivity} tweetActivity The Gnip tweet activity object to fetch languages from
 */
PowertrackDataSource.prototype._parseLangsFromActivity = function(tweetActivity) {
	// Fetch the language codes from both twitter and Gnip data, if present
	var langs = [];

	if (tweetActivity.twitter_lang) langs.push(tweetActivity.twitter_lang);
	if (tweetActivity.gnip && tweetActivity.gnip.language && tweetActivity.gnip.language.value) langs.push(tweetActivity.gnip.language.value);
	return langs;
};

/**
 * Returns text to be tweeted to the user based on the dialogue type
 * @param  {String} dialogue Dialogue Type (ahoy, requests.card)
 * @param  {String} language Text string containing ISO 639-1 two letter language code e.g. 'en', 'id'
 */
PowertrackDataSource.prototype._getDialogue = function(dialogue, language){
	var self = this;
	if (language in dialogue === false) {
		language = self.config.twitter.defaultLanguage;
	}
	return (dialogue[language]);
};

PowertrackDataSource.prototype._ahoy = function(username, disaster, language, callback){
	var self = this;
	callback(null, self._getDialogue(self.config.twitter.dialogue.ahoy[disaster], language));
};

PowertrackDataSource.prototype._getCardLink = function(username, network, language, disasterType, callback) {
	var self = this;

	var card_request = {"username": username,
						"network": network,
						"language": language
					};

  // Get a card from Cognicity server
  request({
    url: self.config.card_server.address,
    method: 'POST',
    headers: {
        'x-api-key': self.config.card_server.x_api_key,
        'Content-Type': 'application/json'
    },
    port: self.config.card_server.port,
    json: true,
    body: card_request
  }, function(error, response, body){
    if (!error && response.statusCode === 200){
      self.logger.info('Fetched card id: ' + body.cardId);
      // Construct the card link to be sent to the user
      // TODO - ADD CODE FOR PREP CARDS
      var cardLink = self.config.front_end.card_url_prefix + body.cardId + "/" + disasterType;
			var messageText =  self._getDialogue(self.config.twitter.dialogue.requests.card, language) + ' ' + cardLink;
			callback(null, messageText);
    } else {
			var err = 'Error getting card: ' + JSON.stringify(error) + JSON.stringify(response);
      self.logger.error(err);
			callback(err, null);
    }
  });
};

// Export the PowertrackDataSource constructor
module.exports = PowertrackDataSource;
