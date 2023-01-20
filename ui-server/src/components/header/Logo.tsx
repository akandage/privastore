import React from 'react';
import * as styles from './Logo.scss';

export default function Logo() {
    return <header className={styles.logo}>
    <img src="images/logo.svg"></img>
    <h1>PrivaStore</h1>
</header>
}