import React from 'react';
import { NavLink } from 'react-router-dom';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faList } from '@fortawesome/free-solid-svg-icons';

class Sidebar extends React.Component {
    render() {
        return (
            <aside>
                <h1>COMP90024 Assignment 2</h1>
                <ul>
                    <li>
                        <NavLink exact activeClassName="selected" to="/">
                            <FontAwesomeIcon icon={faList} />Summary
                        </NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/s1">Scenario 1</NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/s2">Scenario 2</NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/s3">Scenario 3</NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/s4">Scenario 4</NavLink>
                    </li>
                    <li>
                        <NavLink activeClassName="selected" to="/s5">Scenario 5</NavLink>
                    </li>
                </ul>
                <p>Made by: Team 42</p>
            </aside>
        );
    }
}
export default Sidebar;
