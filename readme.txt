Compile:

mvn assembly:assembly -DdescriptorId=jar-with-dependencies

Run:


java -cp target/my-app-1.0-SNAPSHOT-jar-with-dependencies.jar  loader.DataLoader
