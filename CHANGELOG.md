# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]
### Changed
- Externalized build logic from Jenkins to shell script so it can be reused
- Refactored the information returned by the Application Detail API to include the YARN application state and also to return information for jobs that have ended. Made the implementation more performant by using the YARN Resource Manager REST API instead of the CLI.

## [0.2.0] 2016-10-21
### Added
- PNDA-2233 Jupyter notebook plugin added to deployment manager

## [0.1.1] 2016-09-13
### Changes
- Improvements to documentation
- Enhanced CI support

## [0.1.0] 2016-07-01
### First version

## [Pre-release]

### Added

- Add hue endpoint to environment endpoints API
- Application names checked to only contain alphanumeric characters (a-z A-Z 0-9 - and _) because they are used directly in file paths.
- Added ability to discover HDFS namedservices
- Added information field to status reports
- Using an external pacakge repository API instead of internal swift integration
- Application detail API (GET /applications/<application>/detail) now returns YARN IDs assigned to the running tasks for that application.
- Oozie error messages are reported when querying for status of an application creation call.
- Packages are validated on deployment and the error messages reported when querying for status of a package deployment call.
- Added support for opentsdb.json descriptor for creating metrics when deploying applications.
- Callback events are sent to the console data logger.
- Application detail API now completed to return Yarn IDs for any Yarn applications associated with a PNDA application.

### Fixed 

- Return IP address for webhdfs/HTTPFS endpoint instead of hostname
- Timeout calls to package repository at 120 seconds.
- Deploying a package that does not exist in the package repository now results in a useful error message being returned to the caller.
- Fixed defect preventing '-' being used in application names.
- Fix Zookeeper quorum bug issue
- Improve package validation to catch packages without 3 point version numbers and where the folder inside the tar does not match the package name.
- Add list of zookeeper nodes to quorum
- Remove port=8020 for named service
- Oozie creator plugin sets 'oozie.wf.application.path' and 'oozie.coord.application.path' to point at the folder not the xml files.
- Removed some stdout printouts
- Fixed bug preventing recency parameter being used on the repository/packages API.
