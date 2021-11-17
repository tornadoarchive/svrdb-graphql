# svrdb-graphql

This project exposes the SPC severe weather database via a GraphQL API.

## Prerequisites
* Python >= 3.9
* [Docker](https://www.docker.com/get-started)
* [Poetry](https://python-poetry.org/) for dependency management

## Starting the debug server
```
docker-compose up --build
```
if you're starting it up for the first time or if you're adding a dependency. Otherwise
```
docker-compose up
```
## Calling the API
Open up `localhost:8000/graphql`

You should see something similar to (but not exactly the same as) this, the GraphiQL interface:

![GraphiQL](https://raw.githubusercontent.com/graphql/graphiql/main/packages/graphiql/resources/graphiql.jpg)

(Taken from the [GraphiQL](https://github.com/graphql/graphiql) repo.)

If not, something went wrong. :(

You can test by querying the API for all Oklahoma F5 or EF5 tornadoes in the SPC database, by entering the following in the query window of the UI.
```
{
  tornado(state: "OK", magnitude: 5) {
    magnitude,
    startLat,
    startLon,
    datetime,
    fatalities
  }
}
```

Learn more about GraphQL capabilities here: https://graphql.org/learn/
