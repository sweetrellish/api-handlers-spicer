The MarketSharpM API is a RESTful web service that exposes the data as read-only using OData.

com.marketsharp.api.client:
 - CustomAuthenticationHelper.java - Builds the authentication header which must be included in every request.
 - MarketSharpClient.java - Sample client code.
 - MarketSharpCrmApiService.java - Restlet auto-generated code.

com.marketsharp.api.data:
 - Restlet auto-generated code. 

Required Libraries: Restlet Version 2.0 available at http://www.restlet.org/downloads/stable
 - org.restlet.ext.odata.jar
 - org.reslet.jar

Helpful filter options reference: http://msdn.microsoft.com/en-us/library/cc907912.aspx

ContactType Notes:
 - contactType of 1 = Prospect, 2 = Lead, 3 = Customer