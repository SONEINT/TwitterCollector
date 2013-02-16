Tweet Collector
==================

This toolkit provides capabilities for collecting the following data for a given set of Twitter users or hashtags: 
- Historical tweets using [User Timeline API](https://dev.twitter.com/docs/api/1/get/statuses/user_timeline)
- Live tweets using [Streaming API](https://dev.twitter.com/docs/api/1/post/statuses/filter) and [Search API](https://dev.twitter.com/docs/api/1/get/search)
- User's network information (such as friends and followers) using [Friends API](https://dev.twitter.com/docs/api/1/get/friends/ids) and [Followers API](https://dev.twitter.com/docs/api/1/get/followers/ids)
- Changes in user's network (if a friend/follower was added or dropped)
 
Various parameters are available for tuning to define levels of friends/followers to traverse for collecting information for a given user(i.e. number of hops to make in technical sense). Core technologies/frameworks/libraries/API used are: Java, Maven, MongoDB, Twitter4J, Twitter Bootstrap, and Lucene.

### Installation and Setup

Run `mvn package` from inside the main directory to create a WAR file inside the target folder. It can then be deployed to a Tomcat server or a Jetty container by copying the WAR package to the server's webapps folder. Once deployed, open the URL: __http://localhost:8080/twitterDataAcquisition/setup.html__ in your browser (obvisouly, your port # and server address may vary) to specify the input data and configure the following parameters:
- __Database Name__: Name of the database used by MongoDB to store all the collected data.
- __Friend Depth__: # of levels of friends to traverse for getting tweets/network for a given seed user. For e.g. if you want to collect tweets for the seed user and its friends also, set the value to 1. To collect data for only the given seed users, set the value to 0.
- __Follower Depth__: # of levels for the followers.
- __Follow Mentions?__: Boolean flag that specifies if the tweets/network should be collected for the users mentioned in the seed users tweets (using the __@__ character).
- __Seed Users List__ and __Seed Hashtags List__: To specify the input data.
- __Twitter Applications__: Data collection with Twitter API requires Twitter Developer applications for making [OAuth](https://dev.twitter.com/docs/auth/oauth) signed requests. Such applications can be easily created by logging in [dev.twitter.com](https://dev.twitter.com), creating a application, and [generate tokens](https://dev.twitter.com/docs/auth/tokens-devtwittercom) that can be used to make requests. Our toolkit currently uses one Twitter application for one kind of API request. Once the application has been created, press the __Add application__ button and specify the credentials as shown in the screenshot below.

### Setup Screenshot

<img alt="Demo picture" src="http://isi.edu/~shubhamg/tweet-collector-setup.png" width="80%" height="80%">

### Add Applciation Screenshot

<img alt="Demo picture" src="http://isi.edu/~shubhamg/add-application.png" width="80%" height="80%">