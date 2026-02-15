#ifndef MQTT_TEST_MAPPING_H
#define MQTT_TEST_MAPPING_H

/*
 * MQTT requirement-to-test mappings for tests/test_mqtt_conformance.c.
 * Keep this file in sync with the test functions and requirement IDs.
 */

#define MQTT_REQ_MAP(X) \
	X("MQTT-1.5.4-1", test_mqtt_1_5_4_1) \
	X("MQTT-1.5.4-2", test_mqtt_1_5_4_2) \
	X("MQTT-1.5.4-3", test_mqtt_1_5_4_3) \
	X("MQTT-1.5.5-1", test_mqtt_1_5_5_1) \
	X("MQTT-1.5.7-1", test_mqtt_1_5_7_1) \
	X("MQTT-2.1.3-1", test_mqtt_2_1_3_1) \
	X("MQTT-2.2.1-3", test_mqtt_publish_qos1_packet_id_zero) \
	X("MQTT-2.2.1-4", test_mqtt_2_2_1_4_server_publish_packet_id_nonzero) \
	X("MQTT-2.2.1-5", test_mqtt_2_2_1_5_puback) \
	X("MQTT-2.2.1-6", test_mqtt_2_2_1_6_suback) \
	X("MQTT-2.2.2-1", test_mqtt_2_2_2_1) \
	X("MQTT-3.1.0-1", test_mqtt_3_1_0_1) \
	X("MQTT-3.1.0-2", test_mqtt_3_1_0_2) \
	X("MQTT-3.1.2-1", test_mqtt_3_1_2_1_protocol_name) \
	X("MQTT-3.1.2-3", test_mqtt_3_1_2_3) \
	X("MQTT-3.1.2-4", test_mqtt_3_1_2_4_clean_start_discards_session) \
	X("MQTT-3.1.2-5", test_mqtt_3_1_2_5_existing_session) \
	X("MQTT-3.1.2-6", test_mqtt_3_1_2_6_no_existing_session) \
	X("MQTT-3.1.2-7", test_mqtt_3_1_2_7_will_stored) \
	X("MQTT-3.1.2-9", test_mqtt_3_1_2_9_will_requires_payload) \
	X("MQTT-3.1.2-11", test_mqtt_3_1_2_11_will_qos_requires_will_flag) \
	X("MQTT-3.1.2-12", test_mqtt_3_1_2_12_will_qos_range) \
	X("MQTT-3.1.2-13", test_mqtt_3_1_2_13_will_retain_requires_will_flag) \
	X("MQTT-3.1.2-14", test_mqtt_3_1_2_14_will_retain_false) \
	X("MQTT-3.1.2-15", test_mqtt_3_1_2_15_will_retain_true) \
	X("MQTT-3.1.2-16", test_mqtt_3_1_2_16_username_flag_unset) \
	X("MQTT-3.1.2-17", test_mqtt_3_1_2_17_username_flag_set_requires_username) \
	X("MQTT-3.1.2-18", test_mqtt_3_1_2_18_password_flag_unset) \
	X("MQTT-3.1.2-19", test_mqtt_3_1_2_19_password_flag_set_requires_password) \
	X("MQTT-3.1.3-1", test_mqtt_3_1_3_1_connect_payload_order) \
	X("MQTT-3.1.3-3", test_mqtt_3_1_3_3_client_id_missing) \
	X("MQTT-3.1.3-4", test_mqtt_3_1_3_4_client_id_utf8_invalid) \
	X("MQTT-3.1.3-5", test_mqtt_3_1_3_5_client_id_allowed) \
	X("MQTT-3.1.3-11", test_mqtt_3_1_3_11_will_topic_utf8) \
	X("MQTT-3.1.3-12", test_mqtt_3_1_3_12_username_utf8_invalid) \
	X("MQTT-3.1.4-1", test_mqtt_3_1_4_1_connect_too_short) \
	X("MQTT-3.1.4-2", test_mqtt_3_1_4_2_auth_required) \
	X("MQTT-3.1.4-4", test_mqtt_3_1_2_4_clean_start_discards_session) \
	X("MQTT-3.1.4-5", test_mqtt_3_1_4_5_connack_success) \
	X("MQTT-3.1.4-6", test_mqtt_3_1_4_6_reject_ignores_followup) \
	X("MQTT-3.2.0-1", test_mqtt_3_2_0_1_connack_first_packet) \
	X("MQTT-3.2.0-2", test_mqtt_3_2_0_2_single_connack) \
	X("MQTT-3.2.2-1", test_mqtt_3_2_2_1_connack_flags_reserved) \
	X("MQTT-3.2.2-2", test_mqtt_3_2_2_2) \
	X("MQTT-3.2.2-3", test_mqtt_3_2_2_3_existing_session_present) \
	X("MQTT-3.2.2-6", test_mqtt_3_2_2_6_error_clears_session_present) \
	X("MQTT-3.2.2-7", test_mqtt_3_2_2_7_error_reason_closes) \
	X("MQTT-3.2.2-8", test_mqtt_3_2_2_8_connack_reason_code) \
	X("MQTT-3.3.1-1", test_mqtt_3_3_1_1_dup_on_redelivery) \
	X("MQTT-3.3.1-2", test_mqtt_publish_qos0_dup_rejected) \
	X("MQTT-3.3.1-4", test_mqtt_publish_qos_invalid) \
	X("MQTT-3.3.2-1", test_mqtt_3_3_2_1_topic_name_required) \
	X("MQTT-3.3.2-2", test_mqtt_3_3_2_2_topic_name_no_wildcards) \
	X("MQTT-3.3.2-8", test_mqtt_3_3_2_8_topic_alias_zero) \
	X("MQTT-3.3.2-13", test_mqtt_3_3_2_13_response_topic_utf8) \
	X("MQTT-3.3.2-14", test_mqtt_3_3_2_14_response_topic_no_wildcards) \
	X("MQTT-3.3.2-19", test_mqtt_3_3_2_19_content_type_utf8) \
	X("MQTT-3.3.4-1", test_mqtt_3_3_4_1_publish_qos1_puback) \
	X("MQTT-3.6.1-1", test_mqtt_3_6_1_1_pubrel_flags_invalid) \
	X("MQTT-3.8.1-1", test_mqtt_3_8_1_1_subscribe_flags) \
	X("MQTT-3.8.3-1", test_mqtt_3_8_3_1_subscribe_utf8_invalid) \
	X("MQTT-3.8.3-2", test_mqtt_3_8_3_2) \
	X("MQTT-3.8.3-4", test_mqtt_subscribe_shared_no_local_rejected) \
	X("MQTT-3.8.3-5", test_mqtt_subscribe_reserved_bits_rejected) \
	X("MQTT-3.8.4-1", test_mqtt_3_8_4_1_suback_sent) \
	X("MQTT-3.8.4-2", test_mqtt_3_8_4_2_suback_packet_id) \
	X("MQTT-3.8.4-3", test_mqtt_3_8_4_3_subscribe_replaces_existing) \
	X("MQTT-3.8.4-5", test_mqtt_3_8_4_5_suback_single_response) \
	X("MQTT-3.8.4-6", test_mqtt_3_8_4_6_suback_reason_codes) \
	X("MQTT-3.8.4-7", test_mqtt_3_8_4_6_suback_reason_codes) \
	X("MQTT-3.8.4-8", test_mqtt_3_8_4_8_subscribe_qos_minimum) \
	X("MQTT-3.9.3-1", test_mqtt_3_9_3_1_suback_reason_code_order) \
	X("MQTT-3.9.3-2", test_mqtt_3_8_4_6_suback_reason_codes) \
	X("MQTT-3.10.1-1", test_mqtt_2_1_3_1_unsubscribe_flags) \
	X("MQTT-3.10.3-1", test_mqtt_3_10_3_1_unsubscribe_utf8_invalid) \
	X("MQTT-3.10.3-2", test_mqtt_3_10_3_2_unsubscribe_payload_nonempty) \
	X("MQTT-3.10.4-1", test_mqtt_3_10_4_1_unsubscribe_deletes_subscription) \
	X("MQTT-3.10.4-2", test_mqtt_3_10_4_2_unsubscribe_stops_new_messages) \
	X("MQTT-3.10.4-3", test_mqtt_3_10_4_3_unsubscribe_completes_inflight_qos1) \
	X("MQTT-3.10.4-4", test_mqtt_3_10_4_4_unsuback_sent) \
	X("MQTT-3.10.4-5", test_mqtt_3_10_4_5_unsuback_packet_id) \
	X("MQTT-3.10.4-6", test_mqtt_3_10_4_6_unsuback_single_response) \
	X("MQTT-3.11.3-1", test_mqtt_3_11_3_1_unsuback_reason_code_order) \
	X("MQTT-3.11.3-2", test_mqtt_3_11_3_1_unsuback_reason_code_order) \
	X("MQTT-3.12.4-1", test_mqtt_3_12_4_1_pingresp_sent) \
	X("MQTT-3.14.1-1", test_mqtt_3_14_1_1_disconnect_flags_invalid) \
	X("MQTT-3.14.4-1", test_mqtt_3_14_4_1_disconnect_no_more_packets) \
	X("MQTT-3.14.4-2", test_mqtt_3_14_4_2_disconnect_closes) \
	X("MQTT-3.15.1-1", test_mqtt_3_15_1_1_auth_flags_invalid) \
	X("MQTT-4.7.3-1", test_mqtt_4_7_3_1_topic_name)

#define MQTT_POLICY_MAP(X) \
	X("MQTT-3.1.2-2", test_mqtt_3_1_2_2_v3_rejected) \
	X("MQTT-3.1.2-30", test_mqtt_3_1_2_30_auth_method_gates_packets) \
	X("MQTT-3.1.3-6", test_mqtt_3_1_3_6_zero_length_client_id) \
	X("MQTT-3.1.3-8", test_mqtt_3_1_3_8_invalid_client_id) \
	X("MQTT-4.12.0-1", test_mqtt_connect_auth_method_rejected)

#endif
