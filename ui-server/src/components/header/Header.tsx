import React from 'react';
import * as styles from './Header.scss';

import Logo from './Logo';
import RoundButton from '../RoundButton';
import SearchBox from '../SearchBox';

export default function Header() {
    return <header className={styles.header}>
        <Logo />
        <div className={styles.headerSearch}>
            <SearchBox />
        </div>
        <div className={styles.headerControls}>
            <RoundButton img="images/icons/help.svg" tooltip="Help" />

            <RoundButton img="images/icons/settings.svg" tooltip="Settings" />
        </div>
    </header>
}