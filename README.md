COMMENTS: We do not include UNIT tests for this MP.  

HOW TO RUN THE CODE:

1. You need to install golang in https://golang.org/doc/install.

2. You need to complete the "Config.json" in the "/src" for each machine,
"numOfServer" is the number of machines,"numOfThread" is the number of worker threads for the client,
and "Server" should contain the information for every server in the network("Addr" should be the ip address
of the server, and "Port" should be the port number of the server), note that one of the "Addr" must be
"127.0.0.1" so you can talk to yourself.

3. In the bash terminal, go to MP3 and do "make clean" and "make" to generate the executable

4. You should see "command:" in stdout after you begin the program and join all the nodes that you want

5. Use ctrl+c to terminal the program. Otherwise you need to kill the program (because it will still be using the port)

6. We support distributed GREPping (find instructions in MP1 readme), MP2 membership commands (List, Self, Leave), and 5 new commands:

    PUT [sdfsfilename] [Localfilename]
    GET [Localfilename] [sdfsfilename]
    DELETE [sdfsfilename]
    LS [sdfsfilename]
    STORE

    On write-write conflicts (PUTs to the same file within a minute of each other) you will receive a (Y/N) confirmation to PUT the file
