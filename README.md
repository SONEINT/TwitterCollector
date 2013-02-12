Tweet Collector
==================

This toolkit provides capabilities for collecting the following for a given set of users or hashtags: 
- Historical tweets using [User Timeline API](https://dev.twitter.com/docs/api/1/get/statuses/user_timeline)
- Live tweets using [Streaming API](https://dev.twitter.com/docs/api/1/post/statuses/filter) and [Search API](https://dev.twitter.com/docs/api/1/get/search)
- User's network information (such as friends and followers) using [Friends API](https://dev.twitter.com/docs/api/1/get/friends/ids) and [Followers API](https://dev.twitter.com/docs/api/1/get/followers/ids)
- Changes in user's network from Twitter
 
Various parameters are available for tuning to define levels of friends/followers to traverse for collecting information for a given user(i.e. number of hops to make in technical sense). Core technologies/frameworks/libraries/API used are: Java, Maven, MongoDB, Twitter4J, Twitter Bootstrap, and Lucene.

### Installation

Run `mvn package` from inside the main directory to create a WAR file inside the target folder. It can then be deployed to a Tomcat server or a Jetty container by copying the file to their webapps folder. Once deployed, open the URL: *http://localhost:8080/twitterDataAcquisition/setup.html* in your browser to specify input data and confogure the following parametrs:
