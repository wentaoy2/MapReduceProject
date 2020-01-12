COMMENTS: We do not include UNIT tests for this MP.  

HOW TO RUN THE CODE:

1. You need to install golang in https://golang.org/doc/install.

2. You need to complete the "Config.json" in the "/src" for each machine,
"numOfServer" is the number of machines,"numOfThread" is the number of worker threads for the client,
Jsonid should be the id for current machine.

3. In the bash terminal, go src do "make clean" and "make" to generate the executable

4. You should see "command:" in stdout after you begin the program and join all the nodes that you want

5. Use ctrl+c to terminal the program. Otherwise you need to kill the program (because it will still be using the port)

6. We support distributed GREP, ping, membership commands (List, Self, Leave), and:

    PUT [sdfsfilename] [Localfilename]
    GET [Localfilename] [sdfsfilename]
    DELETE [sdfsfilename]
    LS [sdfsfilename]
    STORE

    On write-write conflicts (PUTs to the same file within a minute of each other) you will receive a (Y/N) confirmation to PUT the file

7. We also support map and reduce, note that the files involved in map reduce must be put into the system using PUT command, and the executables must be there for every machine.
command for map: maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>
command for reduce: juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}

