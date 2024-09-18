.PHONY := build


CS3380A2Q5.class: CS3380A2Q5.java 
	javac CS3380A2Q5.java

build: CS3380A2Q5.class 

run: build
	java -cp .:hsqldb.jar CS3380A2Q5
