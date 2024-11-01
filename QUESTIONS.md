Is the program expected to run once to process a file and fill up the database, or should we consider this is a permanent running process that will receive data.

I assume the database should be persistent, i.e. not reinitialized when we restart the docker. Can you please confirm?

How should the raw datafile be passed to the java program?
    - Is it ok to have it mounted within the docker and have the program read the file when it starts?
    - Or, do we want to pass it through some API?

Is it OK to assume we can load the whole file into memory for processing, or should we rather load the data row by row, as we may have much larger files that wouldn't fit into memory?

