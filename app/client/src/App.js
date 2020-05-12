import React from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom';
import Sidebar from './Sidebar';

import './main.scss';

class App extends React.Component {
    render() {
        return (
            <Router>
                <Sidebar />
                <main>
                    <Switch>
                        <Route exact path="/">
                            <h2>Harvest Summary</h2>
                        </Route>
                    </Switch>
                </main>
            </Router>
        );
    }
}

export default App;
