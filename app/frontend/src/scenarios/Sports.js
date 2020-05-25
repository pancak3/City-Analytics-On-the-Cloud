import React, {useEffect, useState} from 'react';
import Scenario from '../components/Scenario';
import {
    getSportsExerciseFreq,
    getSportsExerciseFreqArea
} from '../helper/api';
import PropTypes from 'prop-types';
import {
    ExpansionPanel,
    ExpansionPanelSummary,
    ExpansionPanelDetails,
    Input,
    Typography,
    Button,
    Grid,
    Box,
    TableHead,
    TableBody,
    TableContainer,
    TableCell,
    TableRow,
    Table,
} from '@material-ui/core';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome';
import {faTwitter} from '@fortawesome/free-brands-svg-icons';
import {prepareGeoJSON} from './geo';

function Sports(props) {
    const plainGeo = props.geojson;

    const [counts, setExerciseSports] = useState(null);
    const [loaded, setLoaded] = useState(false);
    const [sportData, setSportData] = useState(null);
    const [geojson, setGeoJSON] = useState(null);
    const [freq, setFreq] = useState(null);
    const [area, setArea] = useState(null);
    const [areaCode, setAreaCode] = useState(null);
    const [freqExpanded, setFreqExpanded] = useState(true);
    const [indExpanded, setIndExpanded] = useState(true);
    const [marker, setMarker] = useState(null);
    const [areaLoaded, setAreaLoaded] = useState(null);

    // Load counts
    useEffect(() => {
        if (loaded) return;
        setLoaded(true);
        getSportsExerciseFreq().then((data) => {
            setExerciseSports(data);
        });

    }, [loaded, plainGeo]);

    // Which data should be used
    const data = sportData ? sportData : counts;

    // When geojson is loaded
    useEffect(() => {
        if (plainGeo && data) {
            setGeoJSON(prepareGeoJSON(plainGeo, data));
        }
    }, [plainGeo, data]);

    // When data is changed, get top 5 frequency
    useEffect(() => {
        if (!data) return;
        const freq = data
            ? Object.keys(data).sort((a, b) => data[b] - data[a])
            : null;
        setFreq(freq.slice(0, 5));
    }, [data]);


    // Get tweets of area
    useEffect(() => {
        if (!areaCode || areaLoaded) return;

        setAreaLoaded(true);
        getSportsExerciseFreqArea(areaCode, sport).then((data) => {
            setArea(data);
            setFreqExpanded(false);
            setSportExpanded(false);
            setIndExpanded(true);
            setMarker(props.areaCentroid ? props.areaCentroid[areaCode] : null);
        });
    }, [areaLoaded, areaCode, sport, props.areaCentroid]);

    return (
        <Scenario
            data={geojson}
            mode={'k'}
            featureClick={(feature) => {
                setAreaCode(feature.properties.feature_code);
                setAreaLoaded(false);
            }}
            marker={marker}
        >
            <div>
                <ExpansionPanel
                    defaultExpanded
                    expanded={freqExpanded}
                    onChange={(e) => setFreqExpanded(!freqExpanded)}
                >
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon/>}>
                        <h5>Mentioned Sports and Exercise </h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        <Grid>
                            {freq ? (
                                freq.length > 0 ? (
                                    <TableContainer>
                                        <Table>
                                            <TableHead>
                                                <TableRow>
                                                    <TableCell component="th">
                                                        SA2 Area
                                                    </TableCell>
                                                    <TableCell component="th">
                                                        Times
                                                    </TableCell>
                                                </TableRow>
                                            </TableHead>
                                            <TableBody>
                                                {freq.map((area_code) => (
                                                    <TableRow key={area_code}>
                                                        <TableCell>
                                                            {props.areaName
                                                                ? props
                                                                    .areaName[
                                                                    area_code
                                                                    ]
                                                                : ''}
                                                        </TableCell>
                                                        <TableCell>
                                                            {data[area_code]}
                                                        </TableCell>
                                                    </TableRow>
                                                ))}
                                            </TableBody>
                                        </Table>
                                    </TableContainer>
                                ) : (
                                    <p className="mt-4">No data found...</p>
                                )
                            ) : (
                                <p className="mt-2">Loading...</p>
                            )}
                        </Grid>
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>

            {area && (
                <div id="indicative">
                    <ExpansionPanel
                        key="sports"
                        defaultExpanded
                        expanded={indExpanded}
                        onChange={(e) => setIndExpanded(!indExpanded)}
                    >
                        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon/>}>
                            <h5>Indicative tweets</h5>
                        </ExpansionPanelSummary>
                        <ExpansionPanelDetails id="indicative">
                            <Grid>
                                <h6 className="mb-1">
                                    {props.areaName[areaCode]} ({areaCode})
                                </h6>
                                <p id="count">
                                    <strong>Number of tweets: </strong>
                                    {data[areaCode] ? data[areaCode] : 0}
                                </p>

                                <Grid>
                                    {area.map((tweet) => (
                                        <div className="tweet" key={tweet.url}>
                                            <p>{tweet.text}</p>
                                            {tweet.url ? (
                                                <React.Fragment>
                                                    <FontAwesomeIcon
                                                        icon={faTwitter}
                                                    />
                                                    <a
                                                        href={tweet.url}
                                                        rel="noopener noreferrer"
                                                        target="_blank"
                                                    >
                                                        {tweet.url}
                                                    </a>
                                                </React.Fragment>
                                            ) : (
                                                <React.Fragment/>
                                            )}
                                        </div>
                                    ))}
                                </Grid>
                            </Grid>
                        </ExpansionPanelDetails>
                    </ExpansionPanel>
                </div>
            )}
        </Scenario>
    );
}

Sports.propTypes = {
    geojson: PropTypes.array,
    areaName: PropTypes.object,
    areaCentroid: PropTypes.object,
};

export default Sports;
