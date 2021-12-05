# svrdb-graphql

This project exposes the SPC severe weather database via a GraphQL API.

### Table of Contents
* [Prerequisites](#prerequisites)<br>
* [Setup](#setup)<br>
* [Starting the debug server](#starting-the-debug-server)<br>
* [Calling the API](#calling-the-api)<br>
* [Querying](#querying)<br>
   * [Fields](#fields)<br>
   * [Arguments](#arguments)<br>
   * [Examples](#examples)<br>
* [Learn More](#learn-more)

## Prerequisites
* Python >= 3.9
* [Docker](https://www.docker.com/get-started)
* [Poetry](https://python-poetry.org/) for dependency management

## Setup

First setup the SPC data files. You have two options:
1) checkout the files from the `data-files` repo:
```
git clone https://github.com/tornadoarchive/data-files.git
```

2) download the files manually from the [SPC archive](https://www.spc.noaa.gov/wcm/#data).

Next you need to setup your environment variable config. Create a copy of the `.env.example` and rename it to `.env`
```
cp .env.example .env
```
Which will make the environment variables in the file available to the graphql server. You must update your `.env` file to reference the directory with the data files by changing the `DATA_FILE_DIR` property to the appropriate directory. You can optionally change the other variables, but they should work as-is.

🚨 **IMPORTANT**🚨 : do _not_ check in the `.env` file! It's included in the `.gitignore` for a reason. This file is meant to be specific to your local settings. Global changes (such as adding an env variable) should be modified first in `.env`, tested, then promoted to `.env.example` and included there so other team members can have the changes available.

Next, the docker scripts require a couple of shell scripts to be executable, so run
```
chmod +x run.sh wait-for-it.sh 
```
(this step should be automated in the future).

Now you're ready to run the server. You have to seed your local database first. To seed all data, run
```
SEED_DB=all docker-compose up --build
```

To seed only tornado data, instead set 
```
SEED_DB=tornado
```

Note: the process of building the docker images and seeding the database will take a few minutes.


## Starting the debug server
If you are adding additional dependencies or changing the directory structure, you must rebuild the docker images by executing:
```
docker-compose up --build
```
Otherwise just
```
docker-compose up
```
will do. If you're modifying the seeding scripts you'll need to use `SEED_DB` to re-seed your database as appropriate.

## Calling the API
Open up `localhost:8000/graphql`

You should see something similar to (but not exactly the same as) this, the GraphiQL interface:

![GraphiQL](https://raw.githubusercontent.com/graphql/graphiql/main/packages/graphiql/resources/graphiql.jpg)

(Taken from the [GraphiQL](https://github.com/graphql/graphiql) repo.)

If not, something went wrong. :(

## Querying

### Fields

All graphQL queries take the following fields:
```
id
datetime
state
fatalities
injuries
loss
closs
```
`loss` is the property damage costs, and `closs` is the crop damage. Tornadoes have the additional following fields:
```
length
width
startLat
startLon
endLat
endLon
segments
magnitude
magnitude_unk
```
**Clarifications**:

`datetime` is in UTC.

`magnitude` is the (E)F rating of the tornado, `magnitude_unk` is a flag for an unknown rating. A non-null `magnitude` with a `magnitude_unk = true` occurs when the rating is determined on a EFU tornado based on property damage estimates `loss` (which SPC systematically filled in the database a few years ago). If you want to filter for EFU tornadoes, the query should be based on `magnitude_unk = true`.

The `segments` field returns you a list of tornado segments within the tornado. Segments share the same data schema as the parent tornado, sans replacing the `segments` property with `counties` (which we'll discuss below). The definition of segments depends on whoever prepared the data. SPC data segments tornado by state; some international locations will not associate segments, in which case the API will return one segment per tornado.

Hail/wind data will have the following fields:
```
lat
lon
magnitude
county
```
`magnitude` here will be hail size/wind speed. County will be discussed below

*Segments and counties*: Each tornado segment can be associated with one more counties. Counties will have the following fields:
```
id
name
state
state_fips
county_fips
```
Note this is only applicable for US severe weather reports.

### Arguments

The query takes a `filter` argument, which will be a json which takes the following attributes:
```
states
years
months
days
hours
datetimeRange
```
Tornadoes will be have the following filter attributes:
```
efs
pathLengthRange
```
Hail,
```
sizeRange
```

All values associated with these attributes will be lists. 

**Range attributes**: Any `*range` attributes will take a list that's converted as follows. I'll use `datetimeRange` as an example:

*Case 1*: in between
```
datetimeRange: ['2011-04-27 12:00:00', '2011-04-28 12:00:00']
-> where datetime >= '2011-04-27 12:00:00' and datetime < '2011-04-28 12:00:00'
```
(note the `<` in the second part of the query).

*Case 2*: with only lower bound
```
datetimeRange: ['2011-04-27 12:00:00', None]
-> where datetime >= '2011-04-27 12:00:00'
```
*Case 3*: with only upper bound
```
datetimeRange: [None, '2011-04-27 12:00:00']
-> where datetime <= '2011-04-27 12:00:00'
```
*Case 4*: not a range at all
```
datetimeRange: ['2011-04-27 12:00:00']
-> where datetime = '2011-04-27 12:00:00'
```


More to come/to be implemented.

Pagination: TBD.

## Examples

1. You want to plot all tornado segments and color the segments based on the parent tornado's rating for tornadoes that occured during the 2011 Super Outbreak between 4/27/11 12Z and 4/28/12 12Z. You want to display the complete aggregate info on the parent tornado (fatalities, path length, width, datetime, and property loss). You also want to show the counties that were affected.
```
{
  tornado(filter: {
    datetimeRange:["2011-04-27 12:00", "2011-04-28 12:00"]
  }) {
    magnitude
    datetime
    fatalities
    length
    width
    loss
    segments {
      startLat
      startLon
      endLat
      endLon
      counties {
        name
        state
        order
      }
    }
  }
}
```

2. You want to display all (E)F4-F5 in Oklahoma history. You don't care about the segments, you just want the complete tornado tracks and aggregate information the same as above. (TODO: update to query states on segment)
```
{
  tornado(filter: {
    states: ["OK"], 
    efs: [4, 5]
  }) {
    magnitude
    datetime
    fatalities
    length
    width
    loss
    startLat
    startLon
    endLat
    endLon
  }
}
```

## Learn more
Learn more about GraphQL capabilities here: https://graphql.org/learn/
