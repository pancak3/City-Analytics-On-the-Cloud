import React, { useEffect, useState, useRef } from 'react';
import Scenario from '../components/Scenario';
import {
    getSportsExerciseFreq,
    getSportsExerciseFreqArea,
} from '../helper/api';
import PropTypes from 'prop-types';
import {
    ExpansionPanel,
    ExpansionPanelSummary,
    ExpansionPanelDetails,
    Button,
    Grid,
    TableHead,
    TableBody,
    TableContainer,
    TableCell,
    TableRow,
    Table,
    ButtonGroup,
} from '@material-ui/core';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faTwitter } from '@fortawesome/free-brands-svg-icons';
import { prepareGeoJSON } from './geo';

const SPORTS = [
    'Cricket',
    'Tennis',
    'Footy',
    'Motorsport',
    'Soccer',
    'Exercise',
];
const SPORTS_ITEMS = [
    [
        'cricket',
        'batsman',
        'bowler',
        't20',
        'odi',
        'mcg',
        'scg',
        'bbl',
        'bigbash',
    ],
    [
        'tennis',
        'australianopen',
        'nadal',
        'djokovic',
        'federer',
        'kyrgios',
        'barty',
        'atp',
        'williams',
    ],
    [
        'afl',
        'footy',
        'aussierules',
        'collingwoodfc',
        'richmond_fc',
        'sydneyswans',
    ],
    [
        'ferrari',
        'vettel',
        'lewis',
        'prix',
        'ricciardo',
        'formula1',
        'f1',
        'ausgp',
        'motogp',
        'motorsport',
    ],
    [
        'fifa',
        'aleague',
        'soccer',
        'liverpool',
        'messi',
        'ronaldo',
        'epl',
        'mufc',
        'ffa',
        'melcity',
        'melbournevictory',
    ],
    ['exercise', 'workout', 'gym', 'yoga', 'jogging', 'aerobics', 'cardio'],
];

function Sports(props) {
    const plainGeo = props.geojson;

    // overall data
    const [loaded, setLoaded] = useState(false);
    const [overallData, setExerciseSports] = useState(null);
    const [geojson, setGeoJSON] = useState(null);
    const [data, setData] = useState(null);

    const [freq, setFreq] = useState(null);

    // current sport
    const [sport, setSport] = useState('All');

    // set area
    const [areaCode, setAreaCode] = useState(null);
    const [marker, setMarker] = useState(null);
    const [tweets, setTweets] = useState(null);
    const [areaLoaded, setAreaLoaded] = useState(null);
    const [areaCount, setAreaCount] = useState(null);

    const [freqExpanded, setFreqExpanded] = useState(true);
    const [indExpanded, setIndExpanded] = useState(true);
    const [areaInfoExpanded, setAreaInfoExpanded] = useState(true);

    // Prevent updates after dismount
    // https://stackoverflow.com/questions/56450975
    const mounted = useRef(true);
    useEffect(() => {
        return () => {
            mounted.current = false;
        };
    }, []);

    // Load counts
    useEffect(() => {
        if (loaded) return;
        setLoaded(true);
        getSportsExerciseFreq().then((data) => {
            if (!mounted.current) return;
            setExerciseSports(data);
        });
    }, [loaded, plainGeo]);

    useEffect(() => {
        if (!plainGeo || !sport || !overallData) return;

        // All sports, sum up for each area
        const data_sum = {};
        if (sport === 'All') {
            for (const area_data of overallData) {
                data_sum[area_data['key']] = area_data['value'].reduce(
                    (a, b) => a + b,
                    0
                );
            }
        } else {
            const sport_index = SPORTS.indexOf(sport);
            for (const area_data of overallData) {
                data_sum[area_data['key']] = area_data['value'][sport_index];
            }
        }

        if (!mounted.current) return;
        setData(data_sum);
        setGeoJSON(prepareGeoJSON(plainGeo, data_sum));
    }, [overallData, sport, plainGeo]);

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
            if (!mounted.current) return;
            setTweets(data.tweets);
            setAreaCount(data.count);
            setMarker(props.areaCentroid ? props.areaCentroid[areaCode] : null);
        });
    }, [areaLoaded, areaCode, sport, props.areaCentroid]);

    useEffect(() => {
        setAreaLoaded(false);
    }, [sport]);

    return (
        <Scenario
            data={geojson}
            mode={'k'}
            featureClick={(feature) => {
                setAreaCode(feature.properties.feature_code);
                setAreaLoaded(false);
                setFreqExpanded(false);
                setIndExpanded(true);
                setAreaInfoExpanded(true);
            }}
            marker={marker}
        >
            <div>
                <ExpansionPanel defaultExpanded>
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>Sport</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        <Grid>
                            <ButtonGroup
                                color="primary"
                                aria-label="outlined primary button group"
                            >
                                <Button
                                    key="All"
                                    color={
                                        sport === 'All'
                                            ? 'primary'
                                            : 'secondary'
                                    }
                                    onClick={(e) => setSport('All')}
                                >
                                    All
                                </Button>
                                {SPORTS.slice(0, 3).map((s) => (
                                    <Button
                                        color={
                                            sport === s
                                                ? 'primary'
                                                : 'secondary'
                                        }
                                        key={s}
                                        onClick={(e) => setSport(s)}
                                    >
                                        {s}
                                    </Button>
                                ))}
                            </ButtonGroup>
                            <ButtonGroup
                                className="mt-1"
                                color="primary"
                                aria-label="outlined primary button group"
                            >
                                {SPORTS.slice(4, 8).map((s) => (
                                    <Button
                                        color={
                                            sport === s
                                                ? 'primary'
                                                : 'secondary'
                                        }
                                        key={s}
                                        onClick={(e) => setSport(s)}
                                    >
                                        {s}
                                    </Button>
                                ))}
                            </ButtonGroup>
                            {sport !== 'All' && (
                                <p className="mt-4 mb-0">
                                    <strong>Keywords: </strong>
                                    {SPORTS_ITEMS[SPORTS.indexOf(sport)].join(
                                        ', '
                                    )}
                                </p>
                            )}
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
                                                        Count
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

            {areaCode && areaCount && (
                <div>
                    <ExpansionPanel
                        defaultExpanded
                        expanded={areaInfoExpanded}
                        onChange={(e) => setAreaInfoExpanded(!areaInfoExpanded)}
                        key="area"
                    >
                        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                            <h5>Area Information</h5>
                        </ExpansionPanelSummary>
                        <ExpansionPanelDetails>
                            <Grid>
                                <p>
                                    <strong>
                                        Percentage of{' '}
                                        {sport === 'All'
                                            ? ' sport/exercise '
                                            : ` ${sport.toLowerCase()} `}{' '}
                                        related tweets:
                                    </strong>
                                    <br />
                                    {((data[areaCode] / areaCount) * 100)
                                        .toString()
                                        .slice(0, 4) + '%'}
                                    {/* <br />
                                    {data[areaCode] + ' ' + areaCount} */}
                                </p>
                            </Grid>
                        </ExpansionPanelDetails>
                    </ExpansionPanel>
                </div>
            )}

            {tweets && (
                <div id="indicative">
                    <ExpansionPanel
                        key="sports"
                        defaultExpanded
                        expanded={indExpanded}
                        onChange={(e) => setIndExpanded(!indExpanded)}
                    >
                        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
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
                                    {tweets.map((tweet) => (
                                        <div className="tweet" key={tweet.url}>
                                            <p>{tweet.doc.full_text}</p>
                                            {tweet.doc.entities.urls.length >
                                            0 ? (
                                                <React.Fragment>
                                                    <FontAwesomeIcon
                                                        icon={faTwitter}
                                                    />
                                                    <a
                                                        href={
                                                            tweet.doc.entities
                                                                .urls[0].url
                                                        }
                                                        rel="noopener noreferrer"
                                                        target="_blank"
                                                    >
                                                        {
                                                            tweet.doc.entities
                                                                .urls[0].url
                                                        }
                                                    </a>
                                                </React.Fragment>
                                            ) : (
                                                <React.Fragment />
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
