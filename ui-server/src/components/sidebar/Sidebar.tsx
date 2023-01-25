import React from 'react';
import { NavLink } from 'react-router-dom';
import * as styles from './Sidebar.scss';

import StorageUse from './StorageUse';
import UploadButton from '../UploadButton';

export namespace Sidebar {
    export function Sidebar() {
        return <div className={styles.sidebar}>
            <div className={styles.uploadButton}>
                <UploadButton />
            </div>
            <Nav>
                <NavItem name="File System" href="/" icon="images/icons/filesystem.svg" />
                <NavItem name="Recent" href="/recent" icon="images/icons/clock.svg" />
                <NavItem name="Uploads" href="/uploads" icon="images/icons/upload.svg" />
                <NavItem name="Trash" href="/trash" icon="images/icons/trash.svg" />
                <NavItem name="Settings" href="/settings" icon="images/icons/settings.svg" />
            </Nav>
            <StorageUse used={7.39} total={15} unit="GB" />
        </div>
    }

    // @ts-ignore
    export function Nav(props) {
        const { children } = props;

        return <nav className={styles.sidebarNav}>
            <ol>
                {
                    // @ts-ignore
                    children
                }
            </ol>
        </nav>
    }

    type NavItemProps = {
        name: string,
        icon: string,
        href: string
    }

    export function NavItem({ name, icon, href }: NavItemProps) {
        return <li className={styles.sidebarNavItem}>
            <NavLink to={href} className={({ isActive }) => isActive ? styles.sidebarActiveLink : undefined}><img src={icon}/><span>{name}</span></NavLink>
        </li>
    }
}