# Change Log
All notable changes to this project will be documented in this file.

## [Unreleased]

## [2.0.0] 2018-08-28
### Added
- PNDA-4560: Add authorization framework and authorize all API calls
- PNDA-4562: Supply user.name when calling package repository

### Changed
- PNDA-4405: Require user and package fields in create application API
- PNDA-4389: Reject packages containing upstart.conf files
- PNDA-4525: Deprecate Ubuntu 14.04
- PNDA-4511: Use a config property to set which spark-submit command to call for spark streaming components
- PNDA-4398: Support Spark 2 for Oozie jobs
- PNDA-4546: Accept username as a URL parameter instead of in the request body and apply basic authorisation so only the user who created an application (or a special admin user defined in the dm-config file) can modify it
- PNDA-4613: Rename user parameter for deployment manager API from user to user.name to match the default knox behaviour
- PNDA-4560: Remove admin_user setting from unit tests
- PNDA-4500: Redesigned application detailed summary and added flink application detailed summary

### Fixed
- PNDA-4218: Fix application deletion when app name is an HDFS username
- PNDA-4012: Add missing application type in application detailed summary
- PNDA-4009: Improve application status naming in application detailed summary
- PNDA-4237: Failure details provided in application summary info if an application fails to submit to YARN
- PNDA-4796: Flink-stop added to stop the flink applications properly

## [1.0.0] 2018-02-10
### Added:
- PNDA-439: Support deploying/running app as specific user
- PNDA-2834: Better and more detailed application status reporting
- PNDA-3654: Support for spark2 streaming applications
- PNDA-4007: Ability to specify default queue configuration for oozie components

### Changed
- PNDA-3555: Place files in HDFS for packages and applications under `/pnda/system/deployment-manager/<packages|applications>`.
- PNDA-3601: Disable emailtext in Jenkins file and replace it with notifier stage and job

### Fixed
- PNDA-3354: Fix error causing exception to appear in log when trying to deploy packages that do not exist
- PNDA-2282: Improved reporting in error scenarios
- PNDA-3613: Deployment manager tests require sudo to run but should not
- PNDA-4056: Automatically sync environment descriptor

## [0.5.0] 2017-11-24
### Added:
- PNDA-3330: Change to use a default application user instead of the hdfs user.
- PNDA-2445: Support for Hortonworks HDP
- PNDA-439: Application create API requires a user to run the application as.
- PNDA-3345: Provide the app_packages HDFS location (from Pillar) to applications deployed with DM
- PNDA-3528: Add default queue placement for oozie jobs.

### Changed
- PNDA-3486: Place files in HDFS for components under `/user/deployment-manager/applications/<user>/<application>/<component>` to avoid potential clashes using the existing path of `/user/<application>`.

## [0.4.0] 2017-05-23
### Added
- PNDA-2729: Added support for spark streaming jobs written in python (pyspark). Use `main_py` instead of `main_jar` in properties.json and specify additional files using `py_files`.
- PNDA-2784: Make tests pass on RedHat

### Changed
- PNDA-2700: Spark streaming jobs no longer require upstart.conf or yarn-kill.py files, default ones are supplied by the deployment manager.
- PNDA-2782: Disabled Ubuntu-only test


## [0.3.0] 2017-01-20
### Fixed
- PNDA-2498: Application package data is now stored in HDFS with a reference to the path only held in the HBase record. This prevents HBase being overloaded with large packages (100MB+).

### Changed
- PNDA-2485: Pinned all python libraries to strict version numbers
- PNDA-2499: Return all exceptions to API caller

## [0.2.1] 2016-12-12
### Changed
- Externalized build logic from Jenkins to shell script so it can be reused
- Refactored the information returned by the Application Detail API to include the YARN application state and also to return information for jobs that have ended. Made the implementation more performant by using the YARN Resource Manager REST API instead of the CLI.

## [0.2.0] 2016-10-21
### Added
- PNDA-2233 Jupyter notebook plugin added to deployment manager

## [0.1.1] 2016-09-13
### Changed
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
