Siti OSS
===========
**Situaional Intelligence Open Source Framework**

####sitioss-reports-powertrack: Module for [sitioss-reports](https://github.com/petabencana/sitioss-reports) module to collect unconfirmed reports from Twitter via Gnip PowerTrack and send verification requests.

### About
Sitioss-reports-powertrack is the NodeJS reports module for collecting relevant tweets via Gnip PowerTrack as part of the Siti OSS Framework, and sending users verification messages via Twitter. For detailed framework documentation see [http://sitioss.info](http://sitioss.info).
This module is not designed to be run standalone but is designed to be run as a submodule of [sitioss-reports](https://github.com/petabencana/sitioss-reports), which can run just with this submodule alone.

### API Documentation
[https://docs.petabencana.id/master-1)

### Dependencies
* [NodeJS](http://nodejs.org) version 4.2.1 or compatible

#### Node Modules
* Check package.json for details

If you're going to commit changes to the JavaScript, be sure to run 'npm test' first - and fix any issues that it complains about, otherwise the build will fail when you push the commit.

### Installation
Please install this as a submodule of [sitioss-reports](https://github.com/petabencana/sitioss-reports). 

Install the node dependencies for this submodule as listed in package.json using npm: `npm install`

#### Platform-specific notes ####
To build on OS X we recommend using [homebrew](http://brew.sh) to install node, npm, and required node modules as follows:
```shell
brew install node
npm install
```

To build on Windows we recommend installing all dependencies and running `npm install`.

### Configuration
App configuration parameters are stored in a configuration file which is parsed by PowertrackDataSource.js. See powertrack-config.js for configuration. For deployment .env files are used to specify default configuration parameters, which are read by the powertrack-config.js file. Currently, these parameters are stored in a global .env file which is loaded by the parent module cognicity-reports.

#### Gnip parameters
* stream [true | false] - set to true to connect to Gnip stream.
* streamTimeout - Gnip stream timeout, should be >30s (in milliseconds)
* username - Gnip username
* password - Gnip password
* streamUrl - URL to fetch JSON stream from Gnip PowerTrack
* rulesUrl - URL to fetch JSON rules from Gnip PowerTrack
* rules - List of objects to configure Gnip PowerTrack rules. Objects contain a series of key-value properties, where the key is the Gnip PowerTrack tag for the rule, and the value is the rule as a string.
* maxReconnectTimeout - Time in seconds that is the longest delay between reconnection attempts for the stream

#### Twitter account configuration
Set the app authentication parameters as provided by Twitter. See the [ntwitter-module](https://github.com/AvianFlu/ntwitter) documentation for more details.
* usernameReplyBlacklist - Twitter usernames (without @, comma separated for multiples) which will never be sent to in response to tweet processing
* usernameVerify - Twitter username (without @) authorised to verify reports via retweet functionality

#### Twitter send parameters
* send_enabled [true | false] - set to true to enable confirmation request tweets to be sent.
* addTimestamp [true | false] - if true, append a timestamp to each sent tweet.
* url_length - The length of the twitter t.co URL shortened URLs (see https://dev.twitter.com/overview/t.co ).

#### Twitter message text
The messages are stored in objects, where the object name is the name of the message.
Within the object, the property name (key) is the language, and the value is the message text.
There is also a top-level 'defaultLanguage' property which is used if the language code from the tweet cannot be resolved.

##### Languages
twitter.defaultLanguage Sets the default language code used in responses if one can't be determined in the tweet.
* in - Bahasa Indonesian (language code from Gnip)
* id - Bahasa Indonesian (language code from Twitter)
* en - English

##### Messages
Messages can be at most 109 characters long if addTimestamp is enabled, or 123 characters long if addTimestamp is disabled.
Note that this length includes one shortened URL at current lengths - see https://dev.twitter.com/overview/t.co for details.
* invite_text - Text for confirmation request tweets
* askforgeo_text - Text for geolocation reminders
* thanks_text - Thank-you message for confirmed tweet

### Development

#### Testing

To run the full set of tests, run:

```shell
npm test
```

This will run the following tests:

##### JSHint

JSHint will run on all JavaScript files in the root folder and test folders.

Running the script:

```shell
npm run jshint
```

This will print an error to the screen if any problems are found.

##### Mocha

Mocha will run all unit tests in the test folder and can be run with the following script:

```shell
npm run mocha
```

The test output will tell you how many tests passed and failed.

#### Git Hooks

There is a git pre-commit hook which will run the 'npm test' command before your commit and will fail the commit if testing fails.

To use this hook, copy the file from 'git-hooks/pre-commit' to '.git/hooks/pre-commit' in your project folder.

```shell
cp git-hooks/pre-commit .git/hooks/
```

#### Documentation

To build the JSDoc documentation run the following npm script:

```shell
npm run build-docs
```

This will generate the API documentation in HTML format in the `docs` folder, where you can open it with a web browser.

#### Test Coverage

To build test code coverage documentation, run the following npm script:

```shell
npm run coverage
```

This will run istanbul code coverage over the full mocha test harness and produce HTML documentation in the directory `coverage` where you can open it with a web browser.

#### Release

The release procedure is as follows:
* Update the CHANGELOG.md file with the newly released version, date, and a high-level overview of changes. Commit the change.
* Create a tag in git from the current head of master. The tag version should be the same as the version specified in the package.json file - this is the release version.
* Update the version in the package.json file and commit the change.
* Further development is now on the updated version number until the release process begins again.

### License
This software is released under the GPLv3 License. See License.txt for details.
