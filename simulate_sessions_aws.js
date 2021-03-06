const axios = require('axios');
const moment = require('moment');
const fs = require('fs');
const https = require('https');
const http = require('http');

// const URL = 'http://localhost:5000/';
// const URL = 'https://flask-monitoring-regressions.herokuapp.com/';
const URL = 'http://18.195.64.56:8000/';
const SIMULATION_LENGTH_IN_MINUTES = 10;
const BASE_TRAFFIC_PER_MINUTE = 100;
const TICKS_PER_SECONDS = 1000;

let REGRESSION_LEVEL = 0;
let REGRESSION_MAGNITUDE = 1;
let DATE = new Date().toISOString().slice(0, 10);

function now() { return new moment(); }
function secondsSince(time) { return now().diff(time, 'seconds'); }

async function sleep(milliseconds) {
    return new Promise((resolve) => {
        setTimeout(resolve, milliseconds);
    })
}

async function startSession(sessionLengthInMinutes) {
    console.log(`Starting session of length ${sessionLengthInMinutes}`);

    const stoppingTime = now().add(sessionLengthInMinutes, 'minutes');

    while (now().isBefore(stoppingTime)) {
        console.log('Sending request');
        await axios.get(URL)
        await sleep(1 + (Math.random() - .5));
    }
}

function requestsPerMinute(minute) {
    const trafficMultiplier = -Math.cos(4 * minute / Math.PI) + 2;
    return trafficMultiplier * BASE_TRAFFIC_PER_MINUTE;
}

(async () => {
    REGRESSION_LEVEL = 2;
    REGRESSION_MAGNITUDE = 1;
    let DB_NAME = `db_${DATE}_A_length_${SIMULATION_LENGTH_IN_MINUTES}_traffic_${BASE_TRAFFIC_PER_MINUTE}_regression_${REGRESSION_LEVEL}_${REGRESSION_MAGNITUDE}.db`;

    console.log(`Setting the regression level...`);
    await axios.get(URL + `set_regression_level/${REGRESSION_LEVEL}`)
    console.log(`Regression level set to ${REGRESSION_LEVEL}.`);

    console.log(`Setting the regression magnitude...`);
    await axios.get(URL + `set_regression_magnitude/${REGRESSION_MAGNITUDE}`)
    console.log(`Regression magnitude set to ${REGRESSION_MAGNITUDE}.`);

    console.log(`Clearing the Database...`);
    await axios.get(URL + `clear_db`)
    console.log(`Database clear.`);
    console.log(`Data will be saved in ${DB_NAME}`)

    console.log(`Starting session of length: ${SIMULATION_LENGTH_IN_MINUTES} minutes...`);
    const simulationStartTime = now();
    const simulationEndTime = simulationStartTime
        .clone()
        .add(SIMULATION_LENGTH_IN_MINUTES, 'minutes');

    const promises = [];
    let nRequests = 0;
    while (now().isBefore(simulationEndTime)) {
        const minute = secondsSince(simulationStartTime) / 60;
        const probability = requestsPerMinute(minute) / (TICKS_PER_SECONDS * 60);
        let progress = Math.round(minute / SIMULATION_LENGTH_IN_MINUTES * 100);

        if (Math.random() < probability) {
            ++nRequests;

            console.log('Sending request nr. ', nRequests, "|", progress, "%");
            axios.get(URL);
        }

        await sleep(1000 / TICKS_PER_SECONDS);
    }

    await Promise.all(promises);
    console.log(`Session finished.`);

    console.log(`Requesting Database...`);
    let new_db_name = DB_NAME;
    http.get(URL + `get_db`, resp => resp.pipe(fs.createWriteStream(new_db_name)));
    // axios.get(URL + `get_db`).then((response) => {
    //     response.data.pipe(fs.createWriteStream(DB_NAME))
    //     // fs.WriteStream(DB_NAME).write(Buffer.from(response.data.toString('base64')));
    // });
    console.log(`Database saved.`);
    console.log(new_db_name);
})();