import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom';
import Sidebar from './Sidebar';
import Summary from './pages/Summary';
import { createMuiTheme, ThemeProvider } from '@material-ui/core/styles';

import './main.scss';
import Exercise from './scenarios/Exercise';
import Word from './scenarios/Word';
import { getGeoJSON } from './helper/api';

const theme = createMuiTheme({
    palette: {
        primary: {
            main: '#3b6aaf',
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
    const [areaNameMapping, setANMapping] = useState(null);
    const [geoLoadRequired, setGeoLoadRequired] = useState(true);

    useEffect(() => {
        if (!geoLoadRequired) return;

        setGeoLoadRequired(false);
        getGeoJSON().then((res) => {
            setGeojson(res);

            const temp = {};
            for (const feature of res) {
                temp[feature.properties.feature_code] =
                    feature.properties.feature_name;
            }
            setANMapping(temp);
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
                            />
                        </Route>
                        <Route path="/scenario/exercise">
                            <Exercise
                                geojson={geojson}
                                areaName={areaNameMapping}
                            />
                        </Route>
                    </Switch>
                </main>
            </Router>
        </ThemeProvider>
    );
}

export default App;
