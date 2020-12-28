const Discord = require("discord.js");
const lineReader = require('line-reader');
const createCsvWriter = require('csv-writer').createArrayCsvWriter;
const csvReader = require('csv-parser')
const https = require('https');
const fs = require('fs');
const config = require("./config.json");
const targz = require('targz');

const client = new Discord.Client();

const prefix = "~";

//comands
/* 
~hashes -> Generate archivehashes.csv & tar.gz
~missing -> missinghashes.txt
~hash (bi directional)
*/

var isReady = false;
var archivehashes = new Map();
var missinghashes = new Array();
const TMP_FILE = "tmp/Cyberpunk2077.log";
var BLOCK_ACTION = false;
var NEEDS_GENERATION = true;

client.on("ready", function () {
    loadArchiveHashes(() => {
        loadMissingHashes(() => {
            isReady = true;
        });
    });
});

async function loadMissingHashes(callback) {

    console.log("Loading missing hashes ...");

    lineReader.eachLine(`data/missinghashes.txt`, function (line) {
        missinghashes.push(line);
    }, function (err) {
        if (err) throw err;
        console.log(`Processing complete! ${missinghashes.length} missing hashes`)

        callback();
    });
}

function loadArchiveHashes(callback) {
    var duplicates = [];
    console.log("Loading archivehashes ...");
    fs.createReadStream(`data/archivehashes.csv`)
        .pipe(csvReader(
            ['path', 'hash']
        ))
        .on('data', (data) => {
            if (archivehashes.has(data.hash)) {
                console.error(`Duplicate hash ${data.hash}`);
                duplicates.push(data);
            } else {
                archivehashes.set(data.hash, data.path);
            }
        })
        .on('end', () => {
            console.log(`Processing complete! ${archivehashes.size} found hashes, ${duplicates.length} duplicates`);
            callback();
        });
}

client.on("message", function (message) {
    if (message.author.bot) return;
    if (message.content.startsWith(prefix)) {
        processCommand(message);
    } else if (message.attachments.size > 0) {
        console.log(message);
        processAttachment(message);
    } else {
        if (!isReady) {
            message.reply("Bot is warming up ...");
            return;
        }
    }
});

/**
 * @param {Discord.Message} message
 */
async function processCommand(message) {
    const commandBody = message.content.slice(prefix.length);
    const args = commandBody.split(' ');
    const command = args.shift().toLowerCase();
    const param = args.shift();

    console.log(param);

    if (command === "missing") {
        var content = "";
        content += "`Missing Hashes:` https://graphicscore.dev/cyberpunk/cyberbot/data/missinghashes.txt\n"
        message.channel.send(content);
    } else if (command === "hashes") {
        if (BLOCK_ACTION) {
            message.reply("I am processing beep boop - please try again in a bit");
            return;
        }

        var progressMessage = await message.channel.send("Generating hash archive ...");

        var content = "";

        content = "> Hash Archive\n";
        content += "`CP77Tools File:` https://graphicscore.dev/cyberpunk/cyberbot/data/archivehashes.csv\n";
        content += "`Targz Archive:` https://graphicscore.dev/cyberpunk/cyberbot/data/archivehashes.tar.gz\n";
        content += "`Missing Hashes:` https://graphicscore.dev/cyberpunk/cyberbot/data/missinghashes.txt\n"

        if (!NEEDS_GENERATION) {
            progressMessage.edit(content);
        } else {
            targz.compress({
                src: 'data/archivehashes.csv',
                dest: 'data/archivehashes.tar.gz'
            }, function(err){
                if(err) {
                    console.log(err);
                } else {
                    NEEDS_GENERATION = false;
                    progressMessage.edit(content);
                }
            });
        }



    } else if (command === "hash" && param.length > 4) {
        var found = false;
        var content = "> Information \n```diff\n"
        if (missinghashes.indexOf(param) != -1) {
            content += `+ String : Unknown\n`;
            content += `- Hash : ${param}`;
            found = true;
        } else if (archivehashes.has(`${param}`)) {
            content += `+ String : ${archivehashes.get(param)}\n`;
            content += `- Hash : ${param}`;
            found = true;
        } else {
            for (let [k, v] of archivehashes) {
                if (v === param) {
                    content += `+ String : ${v}\n`;
                    content += `- Hash : ${k}`;
                    found = true;
                    break;
                }
            }
        }
        content += "```";
        if (!found) {
            message.reply("Unknown Hash");
        } else {
            message.channel.send(content);
        }
    } else {
        message.reply(`Unknown command`);
    }
}

//base\sound\soundbanks\229836153.wem,546252687619239493

/**
 * @param {Discord.Message} message
 */
async function processAttachment(message) {
    var attachment = message.attachments.first();
    if (attachment.size > 0 && attachment.url != null) {
        var progressMessage = await message.channel.send("Processing ...");
        if (attachment.name === "Cyberpunk2077.log") { //potential new hashes
            BLOCK_ACTION = true;
            downloadFile(attachment.url, TMP_FILE, (err) => {
                if (err != null) {
                    progressMessage.edit(`An error occurred : ${err.message}`);
                    BLOCK_ACTION = false;
                } else {
                    var results = new Map();
                    var existingHashCount = 0;
                    fs.createReadStream(TMP_FILE)
                        .pipe(csvReader(
                            ['path', 'hash']
                        ))
                        .on('data', (data) => {
                            if (data.hash != undefined && data.hash.length > 0 && data.path != undefined && data.path.length > 0) {
                                if (results.has(data.hash)) {
                                    console.error(`Duplicate hash ${data.hash}`);
                                } else {
                                    if (!archivehashes.has(data.hash) && missinghashes.indexOf(data.hash) > -1) {
                                        results.set(data.hash, data.path);
                                    } else {
                                        existingHashCount++;
                                    }
                                }
                            }
                        })
                        .on('end', () => {
                            console.log(results);

                            if (results.size > 0) {
                                NEEDS_GENERATION = true;

                                for (let [k, v] of results) {
                                    missinghashes.splice(missinghashes.indexOf(k), 1)
                                    archivehashes.set(k, v);
                                }

                                var file = fs.createWriteStream('data/missinghashes.txt');
                                file.on('error', function (err) { console.err(err) });
                                file.write(missinghashes.join('\r\n'));
                                file.end();

                                const revert = [];
                                for (let [k, v] of archivehashes) {
                                    revert.push([v, k]);
                                }
                                console.log(revert);

                                const csvWriter = createCsvWriter({
                                    path: 'data/archivehashes.csv',
                                    recordDelimiter: '\r\n'
                                });

                                csvWriter.writeRecords(revert)       // returns a promise
                                    .then(() => {
                                        BLOCK_ACTION = false;
                                        progressMessage.edit(`Processing complete! ${results.size} new hashes , ${existingHashCount} existing hashes`);
                                    });
                            } else {
                                progressMessage.edit(`Processing complete! ${results.size} new hashes , ${existingHashCount} existing hashes`);
                            }
                        });
                }
            });
        } else {
            progressMessage.edit(`Unknown file, please upload Cyberpunk2077.log from your bin folder`)
        }
    }
}

function downloadFile(url, dest, cb) {
    var file = fs.createWriteStream(dest);
    var request = https.get(url, function (response) {
        response.pipe(file);
        file.on('finish', function () {
            file.close(cb);  // close() is async, call cb after close completes.
        });
    }).on('error', function (err) { // Handle errors
        fs.unlink(dest); // Delete the file async. (But we don't check the result)
        if (cb) cb(err.message);
    });
};

client.login(config.BOT_TOKEN);