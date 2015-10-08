'use strict';

// Declare app level module which depends on views, and components
angular.module('myApp', [
  'ngRoute','ui.grid',
  'myApp.twitterTrends',  
  'myApp.version'
]).
config(['$routeProvider', function($routeProvider) {
  $routeProvider.otherwise({redirectTo: '/twitter_trends'});
}]);
