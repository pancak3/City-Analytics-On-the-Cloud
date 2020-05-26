// @author Team 42, Melbourne, Steven Tang, 832031

import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom';
import { createMuiTheme, ThemeProvider } from '@material-ui/core/styles';

import './main.scss';

import Sidebar from './Sidebar';
import Summary from './pages/Summary';
import Word from './scenarios/Word';
import Sentiment from './scenarios/Sentiment';
import Sports from './scenarios/Sports';
import { getGeoJSON } from './helper/api';

const theme = createMuiTheme({
    palette: {
        primary: {
            main: '#194585',
        },
        secondary: {
            main: '#5f87c2',
        },
    },
    overrides: {
        MuiTableCell: {
            head: {
                fontWeight: 600,
            },
        },
        MuiExpansionPanelSummary: {
            root: {
                padding: '0 2em',
            },
        },
        MuiExpansionPanelDetails: {
            root: {
                padding: '0 2em',
                paddingBottom: '1.5em',
            },
        },
    },
});

function App() {
    const [geojson, setGeojson] = useState(null);
    const [geoLoadRequired, setGeoLoadRequired] = useState(true);
    const [areaNameMapping, setAreaNameMapping] = useState(null);
    const [areaCentroidMapping, setAreaCentroidMapping] = useState(null);

    useEffect(() => {
        if (!geoLoadRequired) return;

        setGeoLoadRequired(false);
        getGeoJSON().then((res) => {
            setGeojson(res);

            // Area to code to name/centroid mapping
            const area_name = {};
            const area_centroid = {};
            for (const feature of res) {
                area_name[feature.properties.feature_code] =
                    feature.properties.feature_name;
                area_centroid[
                    feature.properties.feature_code
                ] = feature.properties.centroid.reverse();
            }

            setAreaNameMapping(area_name);
            setAreaCentroidMapping(area_centroid);
        });
    }, [geoLoadRequired]);

    return (
        <ThemeProvider theme={theme}>
            <Router>
                <Sidebar />
                <main>
                    <Switch>
                        <Route exact path="/">
                            <Summary />
                        </Route>
                        <Route path="/scenario/word">
                            <Word
                                geojson={geojson}
                                areaName={areaNameMapping}
                                areaCentroid={areaCentroidMapping}
                            />
                        </Route>
                        <Route path="/scenario/sentiment">
                            <Sentiment
                                geojson={geojson}
                                areaName={areaNameMapping}
                                areaCentroid={areaCentroidMapping}
                            />
                        </Route>
                        <Route path="/scenario/sports-exercise">
                            <Sports
                                geojson={geojson}
                                areaName={areaNameMapping}
                                areaCentroid={areaCentroidMapping}
                            />
                        </Route>
                    </Switch>
                </main>
            </Router>
        </ThemeProvider>
    );
}

export default App;
