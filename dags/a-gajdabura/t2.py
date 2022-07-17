bot_token = '2133639967:AAFF6tyNEX-o-KmbG0qPvEJ_7FeCBzLvXIE'    # параметры для моего бота в telegram
chatID = '195275808'
message = 'Проснись и пой, детка'

def send_message(bot_token, chatID, message):     # функция для отпраки сообщения боту в telegram
    import requests
    url = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chatID}&parse_mode=Markdown&text={message}'
    response = requests.get(url)
    return response.json()

send_message = send_message(bot_token, chatID, message)
print(send_message)