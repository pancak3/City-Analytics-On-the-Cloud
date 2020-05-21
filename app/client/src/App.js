import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom';
import Sidebar from './Sidebar';
import Summary from './pages/Summary';

import './main.scss';
import Exercise from './scenarios/Exercise';
import { getGeoJSON } from './helper/api';

function App() {
    const [geojson, setGeojson] = useState(null);
    const [geoLoadRequired, setGeoLoadRequired] = useState(true);

    useEffect(() => {
        if (!geoLoadRequired) return;

        setGeoLoadRequired(false);
        getGeoJSON().then((res) => {
            setGeojson(res);
        });
    }, [geoLoadRequired]);

    return (
        <Router>
            <Sidebar />
            <main>
                <Switch>
                    <Route exact path="/">
                        <Summary />
                    </Route>
                    <Route path="/scenario/exercise">
                        <Exercise geojson={geojson} />
                    </Route>
                </Switch>
            </main>
        </Router>
    );
}

export default App;
