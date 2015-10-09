'use strict';

angular.module('myApp.twitterTrends', ['ngRoute', 'ui.grid','ui.grid.pagination'])

.config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/twitter_trends', {
    templateUrl: 'twitter_trends/twittertrends.html',
    controller: 'TwitterTrendsCtrl'
  });
}])

.controller('TwitterTrendsCtrl', ['$scope','$http', '$timeout', function ($scope, $http, $timeout) {
	
	$scope.gridOptions1 = {
	    paginationPageSizes: [100, 200, 300],
	    paginationPageSize: 100,
	    columnDefs: [
	      { name: 'hashTag' },
	      { name: 'count', type: 'number' }	      
	    ]  
	};
	
	var getTrends = function() { 
		$timeout(function() {
			 $http.get('http://sandbox.hortonworks.com:9090/twittertrendsui/rest/hashtagtrends')
		  	  .success(function (data) {
		  		$scope.gridOptions1.data = data;	   
		  	  });
			 getTrends();
        }, 1000);
	};
	
	getTrends();
	
	var funcOrder = function sort(a, b) {
		if (a.count < b.count) {
		    return -1;
		} else if (a.count > b.count) {
		    return 1;
		}
		else {
		    return 0;
		}
	}
}]);