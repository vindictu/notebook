#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 10/31/2018 10:44 AM
# @Author  : mayuanlin
# @Mail    : ma.vindictu@gmail.com
# @Site    :
# @File    : cmd_sql.py
# @Software: PyCharm


# 运营统计 instance project domain
Operating_statistics = """SELECT
    	domain_tmp. NAME AS domain_name,
    	project_tmp. NAME AS project_name,
    	CAST(SUM(project_tmp.memory_mb)/1024 as decimal (9, 0)) AS allot_mem,
    	SUM(project_tmp.vcpus) AS allot_cpu,
    	count(1) as instance_num
    FROM
    	(
    		SELECT
    			*
    		FROM
    			nj_keystone.project AS domain
    		WHERE
    			is_domain = 1
    		AND enabled = 1
                    AND domain.`name` IN (
    			'咪咕视讯科技有限公司',
    			'咪咕音乐有限公司',
    			'咪咕数字传媒有限公司',
    			'咪咕动漫有限公司',
    			'咪咕文化科技有限公司',
    			'咪咕互动娱乐有限公司'
    		)
    	) AS domain_tmp
    right JOIN (
    	SELECT
    		*
    	FROM
    		(
    			SELECT
    				project_id,
    				memory_mb,
    				vcpus
    			FROM
    				instances
    			WHERE
    				`host` LIKE 'compute%'
    			AND vm_state in ('active', 'stopped', 'error')
    			AND deleted = 0
    		) AS instance
    	LEFT JOIN nj_keystone.project AS kp ON instance.project_id = kp.id
    ) AS project_tmp ON domain_tmp.id = project_tmp.domain_id
    GROUP BY
    	project_tmp. NAME
    ORDER BY
        domain_tmp.name"""

total_cpu_mem = """SELECT
	SUM(vcpus) AS total_cpu,
	CAST(
		SUM(memory_mb) / 1024 AS DECIMAL (9, 0)
	) AS total_mem
FROM
	compute_nodes
WHERE
	hypervisor_type NOT LIKE '%ironic%'
AND deleted = 0"""