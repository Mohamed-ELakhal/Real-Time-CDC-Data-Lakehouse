# ============================================================
#  outputs.tf — CDC Data Lakehouse
# ============================================================

output "instance_public_ip" {
  description = "Public IP address of the EC2 instance."
  value       = aws_instance.cdc_lakehouse.public_ip
}

output "instance_id" {
  description = "EC2 instance ID — use to start/stop from the AWS Console or CLI."
  value       = aws_instance.cdc_lakehouse.id
}

output "ssh_command" {
  description = "Ready-to-paste SSH command. Available ~90 seconds after apply."
  value       = "ssh -i ${var.ssh_private_key_path} ubuntu@${aws_instance.cdc_lakehouse.public_ip}"
}

output "bootstrap_log" {
  description = "Tail the cloud-init bootstrap progress after SSHing in."
  value       = "tail -f /var/log/cdc-lakehouse-init.log"
}

output "service_urls" {
  description = "Browser / client URLs for each service once ./startup.sh has finished."
  value = {
    airflow_ui        = "http://${aws_instance.cdc_lakehouse.public_ip}:8080"
    kafka_connect_api = "http://${aws_instance.cdc_lakehouse.public_ip}:8083"
    clickhouse_http   = "http://${aws_instance.cdc_lakehouse.public_ip}:8123"
    clickhouse_native = "${aws_instance.cdc_lakehouse.public_ip}:9000"
    kafka_bootstrap   = "${aws_instance.cdc_lakehouse.public_ip}:9092"
    postgres          = "${aws_instance.cdc_lakehouse.public_ip}:5432"
    mongodb           = "${aws_instance.cdc_lakehouse.public_ip}:27017"
  }
}

output "stop_instance_command" {
  description = "Stop the EC2 instance — halts compute billing while keeping the EBS disk. Resume with start_instance_command. Note: public IP changes on restart."
  value       = "aws ec2 stop-instances --instance-ids ${aws_instance.cdc_lakehouse.id} --region ${var.aws_region}"
}

output "start_instance_command" {
  description = "Start the EC2 instance again. Run `terraform output ssh_command` after starting to get the new public IP."
  value       = "aws ec2 start-instances --instance-ids ${aws_instance.cdc_lakehouse.id} --region ${var.aws_region}"
}

output "get_new_ip_after_start" {
  description = "Retrieve the new public IP after restarting the instance."
  value       = "aws ec2 describe-instances --instance-ids ${aws_instance.cdc_lakehouse.id} --region ${var.aws_region} --query 'Reservations[0].Instances[0].PublicIpAddress' --output text"
}

output "destroy_command" {
  description = "Destroy all resources — VPC, subnet, security group, instance, key pair, EBS disk. Stops all billing immediately."
  value       = "terraform destroy -auto-approve"
}
