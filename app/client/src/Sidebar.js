import React from 'react';
import { NavLink } from 'react-router-dom';

class Sidebar extends React.Component {
    render() {
        return (
            <aside>
                <h1>COMP90024 Assignment 2</h1>
                <ul>
                    <li>
                        <NavLink to="/">Summary</NavLink>
                    </li>
                    <li>
                        <NavLink to="/s1">Scenario 1</NavLink>
                    </li>
                    <li>
                        <NavLink to="/s2">Scenario 2</NavLink>
                    </li>
                    <li>
                        <NavLink to="/s3">Scenario 3</NavLink>
                    </li>
                    <li>
                        <NavLink to="/s4">Scenario 4</NavLink>
                    </li>
                    <li>
                        <NavLink to="/s5">Scenario 5</NavLink>
                    </li>
                </ul>
            </aside>
        );
    }
}
export default Sidebar;
