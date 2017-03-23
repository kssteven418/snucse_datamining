JAVA=java
JAVAC=javac
JAR=jar
LIB_CLASSPATH="lib/*"
COMPILE_CLASSPATH="src:${LIB_CLASSPATH}"
RUN_CLASSPATH="build/hw1.jar:${LIB_CLASSPATH}"

SOURCES=$(shell find . -type f -name '*.java')

MAPREDUCE_OPTS=-Dmapreduce.local.map.tasks.maximum=3 -Dmapreduce.local.reduce.tasks.maximum=3 -Dmapreduce.job.reduces=3

build:
	rm -rf build
	mkdir build
	mkdir build/classes
	$(JAVAC) -classpath $(COMPILE_CLASSPATH) -d build/classes $(SOURCES)
	$(JAR) cf build/hw1.jar -C build/classes .
	echo "Build Complete!"

wordcount:
	rm -rf ./output-wordcount
	$(JAVA) -cp $(RUN_CLASSPATH) homework.WordCount $(MAPREDUCE_OPTS) ./data/wordcount ./output-wordcount

tf:
	rm -rf ./output-tf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.TermFrequency $(MAPREDUCE_OPTS) ./data/tfidf ./output-tf

idf:
	rm -rf ./output-idf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.InverseDocumentFrequency $(MAPREDUCE_OPTS) ./data/tfidf ./output-idf 5006

tfidf:
	rm -rf ./output-tfidf
	$(JAVA) -cp $(RUN_CLASSPATH) homework.TFIDFJoin $(MAPREDUCE_OPTS) ./output-tf ./output-idf ./output-tfidf

clean:
	rm -rf build

