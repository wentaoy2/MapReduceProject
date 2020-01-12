# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
MAIN_FILE_NAMES = src/Main.go src/inputandlogging.go src/TopologyAndConfig.go src/Global.go src/ProtocolandHandler.go src/master.go src/master_helpers.go src/master_globals.go src/node.go src/replication.go src/juice_helpers.go
MAIN_BINARY_NAMES=Main inputandlogging TopologyAndConfig Global ProtocolandHandler master master_helpers master_globals node replication juice_helpers

all: Main
Main:
	$(GOBUILD) $(MAIN_FILE_NAMES)
clean:
	$(GOCLEAN)
	rm -f $(MAIN_BINARY_NAMES)
