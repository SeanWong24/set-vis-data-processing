import { csvParse } from "https://cdn.skypack.dev/d3-dsv@2.0.0";
import { DB } from "https://deno.land/x/sqlite@v2.1.1/mod.ts";

const databaseFilePath = "./arbnb.db";
const databaseName = "arbnb";

const variables = [
    "room_id",
    "host_id",
    "room_type",
    "borough",
    "neighborhood",
    "reviews",
    "overall_satisfaction",
    "accommodates",
    "bedrooms",
    "price",
    "minstay",
    "latitude",
    "longitude",
    "last_modified"
];

try {
    await Deno.remove(databaseFilePath, { recursive: true });
} catch { }

const db = new DB(databaseFilePath);
db.query(
    `CREATE TABLE IF NOT EXISTS ${databaseName} (` +
    variables.map(variable => `${variable.replace(/\s/g, '')} ${obtainVariableDataType(variable)},`).join(" ") + " " +
    "PRIMARY KEY(room_id, last_modified)" +
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
            variables.map(variable => variable === "last_modified" ? new Date(datum[variable]).toISOString() : datum[variable])
        );
    }
}

db.close();

function obtainVariableDataType(variableName: string) {
    switch (variableName) {
        case "room_id":
        case "host_id":
        case "room_type":
        case "borough":
        case "neighborhood":
        case "last_modified":
            return "VARCHAR";
        case "reviews":
        case "overall_satisfaction":
        case "accommodates":
        case "bedrooms":
        case "price":
        case "minstay":
        case "latitude":
        case "longitude":
            return "FLOAT";
    }
}

// TODO pass input and output paths as arguments