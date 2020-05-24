import React, { useEffect, useState } from 'react';
import PropTypes from 'prop-types';
import Scenario from '../components/Scenario';
import { getSentiment, getSentimentArea } from '../helper/api';
import { prepareGeoJSON } from './geo';
import {
    ExpansionPanel,
    ExpansionPanelSummary,
    ExpansionPanelDetails,
    Input,
    Typography,
    Button,
    Grid,
    Box,
} from '@material-ui/core';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

function Sentiment(props) {
    const plainGeo = props.geojson;

    const [geojson, setGeoJSON] = useState(null);
    const [overallLoaded, setOverallLoaded] = useState(false);
    const [overall, setOverall] = useState(null);
    const [marker, setMarker] = useState(null);
    const [settingsExpanded, setSettingsExpanded] = useState(true);

    useEffect(() => {
        if (overallLoaded) return;
        setOverallLoaded(true);
        getSentiment().then((data) => {
            setOverall(data);
        });
    }, [overallLoaded]);

    // When data or geojson is loaded
    useEffect(() => {
        if (plainGeo && overall) {
            console.log(overall);
            setGeoJSON(prepareGeoJSON(plainGeo, overall));
        }
    }, [plainGeo, overall]);

    // Get information about area
    function getArea(feature_code) {
        getSentimentArea(feature_code).then((data) => {
            setMarker(
                props.areaCentroid ? props.areaCentroid[feature_code] : null
            );
        });
    }

    return (
        <Scenario
            mode={'k'}
            geojson={geojson}
            featureClick={(feature) => {
                getArea(feature.properties.feature_code);
            }}
            marker={marker}
        >
            <div>
                <ExpansionPanel
                    defaultExpanded
                    expanded={settingsExpanded}
                    onChange={(e) => setSettingsExpanded(!settingsExpanded)}
                >
                    <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />}>
                        <h5>AURIN Data Selection</h5>
                    </ExpansionPanelSummary>
                    <ExpansionPanelDetails>
                        <Grid></Grid>
                    </ExpansionPanelDetails>
                </ExpansionPanel>
            </div>
        </Scenario>
    );
}

Scenario.propTypes = {
    geojson: PropTypes.array,
    areaName: PropTypes.object,
    areaCentroid: PropTypes.object,
};

export default Sentiment;
