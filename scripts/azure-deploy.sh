#!/bin/bash
# ============================================================
# NileFlow Azure VM Deployment Script
# Run this ONCE after SSH-ing into your Azure VM
# ============================================================
set -e

echo "=========================================="
echo "  NileFlow Azure VM Setup"
echo "=========================================="

# --- Step 1: Update system ---
echo "[1/6] Updating system packages..."
sudo apt-get update -y && sudo apt-get upgrade -y

# --- Step 2: Install Docker ---
echo "[2/6] Installing Docker..."
sudo apt-get install -y ca-certificates curl gnupg lsb-release

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add current user to docker group (no sudo needed for docker commands)
sudo usermod -aG docker $USER

# --- Step 3: Install Docker Compose (standalone) ---
echo "[3/6] Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
  -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# --- Step 4: Clone NileFlow ---
echo "[4/6] Cloning NileFlow repository..."
cd ~
if [ -d "NileFlow" ]; then
  echo "NileFlow directory exists, pulling latest..."
  cd NileFlow && git pull
else
  git clone https://github.com/mohamed-mahmoud-de/NileFlow.git
  cd NileFlow
fi

# --- Step 5: Create .env file ---
echo "[5/6] Setting up environment..."
if [ ! -f .env ]; then
  cp .env.example .env
  echo ""
  echo "================================================"
  echo "  IMPORTANT: Edit your .env file now!"
  echo "  nano .env"
  echo "  Add your TOMTOM_API_KEY and DISCORD_WEBHOOK_URL"
  echo "================================================"
  echo ""
  read -p "Press Enter after you've edited .env, or Ctrl+C to do it later..."
fi

# --- Step 6: Start NileFlow ---
echo "[6/6] Starting NileFlow..."
sudo docker-compose up -d

echo ""
echo "=========================================="
echo "  NileFlow is starting up!"
echo "=========================================="
echo ""
echo "  Wait 2-3 minutes for all services to initialize."
echo ""
echo "  Then submit Spark jobs:"
echo "    sudo docker exec nileflow-spark-master /opt/spark/bin/spark-submit \\"
echo "      --master spark://spark-master:7077 \\"
echo "      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \\"
echo "      /opt/nileflow/spark/streaming/traffic_processor.py"
echo ""
echo "  Dashboard:  http://$(curl -s ifconfig.me):8501"
echo "  Airflow:    http://$(curl -s ifconfig.me):8080"
echo "  Spark UI:   http://$(curl -s ifconfig.me):8081"
echo ""
echo "=========================================="
