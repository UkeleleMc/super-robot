#!/usr/bin/env bash

echo "Updating packages..."
sudo apt update && sudo apt upgrade -y

echo "Installing Python and screen..."
sudo apt install -y python3 python3-pip screen

echo "Installing requirements..."
pip3 install --upgrade pip
pip3 install -r requirements.txt

echo "Starting bot in persistent loop..."

cat > runbot.sh <<'EOF'
#!/usr/bin/env bash

while true
do
    echo "Starting bot..."
    python3 bot.py

    echo "Bot crashed. Restarting in 5 seconds..."
    sleep 5
done
EOF

chmod +x runbot.sh

# Start in detached screen session
screen -dmS telegrambot ./runbot.sh

echo "Bot is running in screen session: telegrambot"
echo "To view:"
echo "screen -r telegrambot"
