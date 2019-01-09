/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.plan.node.PlanNode;
import org.apache.commons.lang.StringUtils;

public class NamedField {
    private final String table;
    private final String name;
    private final int hashCode;
    // which node of the field belong
    public final PlanNode planNode;

    public NamedField(String inputTable, String name, PlanNode planNode) {
        String tempTableName;
        if (DbleServer.getInstance().getSystemVariables().isLowerCaseTableNames()) {
            tempTableName = inputTable == null ? null : inputTable.toLowerCase();
        } else {
            tempTableName = inputTable;
        }
        this.table = tempTableName;
        this.name = name;
        this.planNode = planNode;

        //init hashCode
        int prime = 2;
        int hash = tempTableName == null ? 0 : tempTableName.hashCode();
        this.hashCode = hash * prime + (name == null ? 0 : name.toLowerCase().hashCode());
    }

    @Override
    public int hashCode() {

        return hashCode;
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof NamedField))
            return false;
        NamedField other = (NamedField) obj;
        return StringUtils.equals(table, other.table) && StringUtils.equalsIgnoreCase(name, other.name);
    }

    @Override
    public String toString() {
        return "table:" + table + ",name:" + name;
    }
}
