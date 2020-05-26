// @author Team 42, Melbourne, Steven Tang, 832031

import axios, {AxiosRequestConfig} from 'axios';

const BASE_URL =
    process.env.REACT_APP_URL === undefined
        ? 'http://localhost:3000'
        : process.env.REACT_APP_URL;

const getRequest = (url: string, options?: AxiosRequestConfig) => {
    return new Promise((resolve, reject) => {
        axios.get(`${BASE_URL}${url}`, options).then((response) => {
            return resolve(response.data);
        }).catch((err) => {
            return reject(err);
        })
    })
}

// summary page: stats
export function getStats() {
    return getRequest(`/api/stats`);
}

// summary page: general info
export function getGeneralInfo() {
    return getRequest(`/api/general`);
}

// geojson
export function getGeoJSON() {
    return getRequest(`/api/scenarios/geojson`);
}

// keywords page: general tweet counts
export function getCounts() {
    return getRequest(`/api/scenarios/counts`);
}

// keywords page: top 10 hashtags
export function getHashTags() {
    return getRequest(`/api/scenarios/hashtags/all`);
}

// keywords page: top 10 hashtags by area
export function getHashTagsByArea(area: string) {
    return getRequest(`/api/scenarios/hashtags/${area}`);
}

// keywords page: get by keyword
export function getKeyword(keyword: string) {
    return getRequest(`/api/scenarios/keyword/all`, {params: {keyword}});
}

// keywords page: get by keyword and area (returns tweets)
export function getKeywordArea(keyword: string, area: string) {
    return getRequest(`/api/scenarios/keyword/${area}`, keyword ? {params: {keyword}} : {})
}

// sentiment
export function getSentiment() {
    return getRequest(`/api/scenarios/sentiment`);
}

// get sports and exercise
export function getSportsExerciseFreq() {
    return getRequest(`/api/scenarios/sports-exercise`);
}

// get sports and exercise by area
export function getSportsExerciseFreqArea(areaCode: string, sport: string) {
    return getRequest(`/api/scenarios/sports-exercise/${areaCode}?sport=${sport.toLowerCase()}`);
}
