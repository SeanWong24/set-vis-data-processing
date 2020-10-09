import { csvParse } from "https://cdn.skypack.dev/d3-dsv@2.0.0";
import { DB } from "https://deno.land/x/sqlite@v2.1.1/mod.ts";

const variables = [
    "Date",
    "Longitude",
    "Latitude",
    "Elevation",
    "Max Temperature",
    "Min Temperature",
    "Precipitation",
    "Wind",
    "Relative Humidity",
    "Solar"
];
const databaseFilePath = "./weather.db";
const databaseName = "weather";

try {
    await Deno.remove(databaseFilePath, { recursive: true });
} catch { }

const db = new DB(databaseFilePath);
db.query(
    `CREATE TABLE IF NOT EXISTS ${databaseName} (` +
    variables.map(variable => `${variable.replace(/\s/g, '')} ${variable === "Date" ? "VARCHAR" : "FLOAT"},`).join(" ") + " " +
    "PRIMARY KEY(Date, Latitude, Longitude)" +
    ")",
);


const dataDirectory = "./data";
const dataDirectoryEntries = Deno.readDir(dataDirectory);
for await (const { name: fileName } of dataDirectoryEntries) {
    console.log(`Processing ${fileName}...`);
    const fileContent = await Deno.readTextFile([dataDirectory, fileName].join("/"));
    const data = csvParse(fileContent, undefined);
    for (const datum of data) {
        db.query(
            `INSERT INTO ${databaseName} VALUES (${variables.map(() => "?").join(", ")})`,
            variables.map(variable => variable === "Date" ? new Date(datum[variable]).toISOString().slice(0, 10) : datum[variable])
        );
    }
}

db.close();

// TODO pass input and output paths as arguments