import React from 'react';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom';
import Sidebar from './Sidebar';
import Summary from './Summary';

import './main.scss';
import Exercise from './Exercise';

class App extends React.Component {
    render() {
        return (
            <Router>
                <Sidebar />
                <main>
                    <Switch>
                        <Route exact path="/">
                            <Summary />
                        </Route>
                        <Route path="/scenario/exercise">
                            <Exercise />
                        </Route>
                    </Switch>
                </main>
            </Router>
        );
    }
}

export default App;
