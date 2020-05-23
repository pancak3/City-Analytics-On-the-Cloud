import React, { useEffect, useState } from 'react';
import Scenario from '../components/Scenario';
import { getCounts, getKeyword, getKeywordArea } from '../helper/api';
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
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faTwitter } from '@fortawesome/free-brands-svg-icons';

// Adds values to geojson
const prepareGeoJSON = (geojson, data) => {
    return geojson && data
        ? geojson.map((feature) => {
              return {
                  ...feature,
                  properties: {
                      ...feature.properties,
                      feature_value: data[feature.properties.feature_code],
                  },
              };
          })
        : null;
};

function Word(props) {
    const plainGeo = props.geojson;

    const [counts, setCounts] = useState(null);
    const [loaded, setLoaded] = useState(false);
    const [keyword, setKeyword] = useState(false);
    const [keywordData, setKeywordData] = useState(null);
    const [keywordLoaded, setKeywordLoaded] = useState(true);
    const [geojson, setGeoJSON] = useState(null);
    const [freq, setFreq] = useState(null);
    const [area, setArea] = useState(null);
    const [areaCode, setAreaCode] = useState(null);
    const [freqExpanded, setFreqExpanded] = useState(true);
    const [keywordExpanded, setKeywordExpanded] = useState(true);
    const [indExpanded, setIndExpanded] = useState(true);
    const [marker, setMarker] = useState(null);

    // Load counts
    useEffect(() => {
        if (loaded) return;
        setLoaded(true);
        getCounts().then((data) => {
            setCounts(data);
        });
    }, [loaded, plainGeo]);

    // Load keywords
    useEffect(() => {
        if (!keyword || keywordLoaded) return;
        setKeywordLoaded(true);
        getKeyword(keyword).then((data) => {
            setKeywordData(data);
        });
    }, [keyword, keywordLoaded, plainGeo]);

    // Which data should be used
    const data = keywordData ? keywordData : counts;

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

    // Query for keyword
    function submit_keyword() {
        if (keyword === '' || !keyword) {
            setKeywordLoaded(true);
            setKeywordData(null);
            setGeoJSON(prepareGeoJSON(plainGeo, counts));
            setFreqExpanded(true);
            setIndExpanded(false);
        } else {
            // load keyword data
            setKeywordLoaded(false);
            setFreqExpanded(true);
            setIndExpanded(false);
        }
    }

    // Get tweets of area
    function getArea(feature_code) {
        getKeywordArea(keyword, feature_code).then((data) => {
            setArea(data);
            setAreaCode(feature_code);
            setFreqExpanded(false);
            setKeywordExpanded(false);
            setIndExpanded(true);
            setMarker(
                props.areaCentroid ? props.areaCentroid[feature_code] : null
            );
        });
    }

    return (
        <Scenario
            data={geojson}
            mode={'k'}
            featureClick={(feature) => {
                getArea(feature.properties.feature_code);
            }}
            marker={marker}
        >
            <div>
                <ExpansionPanel
                    defaultExpanded
                    expanded={keywordExpanded}
                    onChange={(e) => setKeywordExpanded(!keywordExpanded)}
                >
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>Keyword Search</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        <Grid>
                            <Typography>
                                Search by keyword (SA2 areas)
                            </Typography>
                            <Box mt={1}>
                                <Grid className="d-flex">
                                    <Input
                                        margin="none"
                                        placeholder="Keyword"
                                        onChange={(e) => {
                                            setKeyword(
                                                e.target.value.toLowerCase()
                                            );
                                        }}
                                    ></Input>
                                    <Button
                                        variant="outlined"
                                        color="primary"
                                        onClick={() => submit_keyword()}
                                    >
                                        Search
                                    </Button>
                                </Grid>
                            </Box>
                        </Grid>
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>
            <div>
                <ExpansionPanel
                    defaultExpanded
                    expanded={freqExpanded}
                    onChange={(e) => setFreqExpanded(!freqExpanded)}
                >
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>Frequency</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
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
                                                    Count
                                                </TableCell>
                                            </TableRow>
                                        </TableHead>
                                        <TableBody>
                                            {freq.map((area_code) => (
                                                <TableRow key={area_code}>
                                                    <TableCell>
                                                        {props.areaName
                                                            ? props.areaName[
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
                                <p>No data found...</p>
                            )
                        ) : (
                            <p>Loading...</p>
                        )}
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>
            {area ? (
                <div>
                    <ExpansionPanel
                        defaultExpanded
                        expanded={indExpanded}
                        onChange={(e) => setIndExpanded(!indExpanded)}
                    >
                        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                            <h5>Indicative tweets</h5>
                        </ExpansionPanelSummary>
                        <ExpansionPanelDetails id="indicative">
                            <Grid>
                                <h6>
                                    {props.areaName[areaCode]} ({areaCode})
                                </h6>
                                <p id="count">
                                    <strong>Number of tweets: </strong>
                                    {data[areaCode] ? data[areaCode] : 0}
                                </p>

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
                                            <React.Fragment />
                                        )}
                                    </div>
                                ))}
                            </Grid>
                        </ExpansionPanelDetails>
                    </ExpansionPanel>
                </div>
            ) : (
                <React.Fragment />
            )}
        </Scenario>
    );
}

Word.propTypes = {
    geojson: PropTypes.array,
    areaName: PropTypes.object,
    areaCentroid: PropTypes.object,
};

export default Word;