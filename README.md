# onlineShopDatabase - NEU CS5200 Class Project-2

## Build from source and run

We've included the jdbc connector in `out/production/onlineShopDatabase/mysql-connector-java-8.0.19.jar` and all it takes to build the project is running

```sh
javac -cp "out/production/onlineShopDatabase/mysql-connector-java-8.0.19.jar" -d "out/production/onlineShopDatabase" "src/*.java"
```

and to run the Main class to trigger the operations script, you need to run

```sh
java -cp "out/production/onlineShopDatabase:out/production/onlineShopDatabase/mysql-connector-java-8.0.19.jar" Main
```

These two steps are included in the `run.sh` file and you can just give execution permission to it (by running `chmod +x run.sh`), and just run `run.sh` from commandline.
