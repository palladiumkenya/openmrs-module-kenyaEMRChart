package org.openmrs.module.kenyaemrCharts.metadata;

import org.openmrs.module.metadatadeploy.bundle.AbstractMetadataBundle;
import org.openmrs.module.metadatadeploy.bundle.Requires;
import org.springframework.stereotype.Component;

import static org.openmrs.module.metadatadeploy.bundle.CoreConstructors.*;

/**
 * Implementation of access control to the app.
 */
@Component
@Requires(org.openmrs.module.kenyaemr.metadata.SecurityMetadata.class)
public class ETLAdminSecurityMetadata extends AbstractMetadataBundle{

    public static class _Privilege {
        public static final String APP_ETL_ADMIN = "App: kenyaemretladmin.home";
    }

    public static final class _Role {
        public static final String APPLICATION_ETL_ADMIN = "ETL Administration";
    }

    /**
     * @see AbstractMetadataBundle#install()
     */
    @Override
    public void install() {

        install(privilege(_Privilege.APP_ETL_ADMIN, "Able to refresh and/or recreate ETL tables"));
        install(role(_Role.APPLICATION_ETL_ADMIN, "Can access ETL Admin app",
                idSet(org.openmrs.module.kenyaemr.metadata.SecurityMetadata._Role.API_PRIVILEGES_VIEW_AND_EDIT),
                idSet( _Privilege.APP_ETL_ADMIN
        )));
    }
}
