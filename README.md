# Foxyblox: A Cloud-Based Reliable Storage System
Foxyblox is an application aiming to increase the reliability of storage on the cloud by adding a layer of abstraction between users and the underlying storage system. It allows you to easily distribute your files across multiple locations so that if any one of them fails, the data can still be recovered. Currently, Foxyblox is runnable as a stand-alone binary executable that takes command-line arguments, with no support for adding files anywhere but locally (i.e. this can be run on AWS with plugged in EBS volumes, but cannot take paths to Google cloud storage locations as of yet).

## Code Overview
### fileutils/
This contains the code that actually splits and distributes input files.

### database/
This is the implementation of the database that Foxyblox uses.

### cron/
This contains any tasks that can be run as Cron jobs.

### bash/
This is the command-line functionality - this parses commands given and executes the appropriate functionality.

### types/
This defines basic types used across the packages.

### server/
This contains code for running a basic web server with a file upload option - this was used for testing purposes.

### client/
This contains the client code for sending files to the server above, to measure upload times.

### main.go
Entrypoint of the code, runs the command-line by default.

## Overall
Each folder/package contains a file called <package_name>_test.go. These contain the testing code for each package.