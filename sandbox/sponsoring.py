import random
from colorama import init, Fore, Back, Style

# Initialize colorama
init(autoreset=True)

sponsor_messages = [
    "Enjoying secfsdstools? Please consider sponsoring the project!",
    "Love the tool? Your support keeps development alive – consider sponsoring!",
    "If you find this tool useful, a sponsorship would be greatly appreciated!",
    "Help us continue to improve secfsdstools by becoming a sponsor!",
    "Support open source: Sponsor secfsdstools today!",
    "Keep the updates coming – sponsor secfsdstools and fuel further development.",
    "Like what you see? Consider sponsoring to help drive innovation.",
    "Your support makes a difference. Please sponsor secfsdstools!",
    "Sponsor secfsdstools and help us build a better tool for everyone.",
    "Help keep this project thriving – consider becoming a sponsor.",
    "Support innovation and open source by sponsoring secfsdstools.",
    "Your sponsorship ensures continued updates. Thank you for your support!",
    "Love our daily updates? Become a sponsor and back our work.",
    "Sponsorship fuels open-source progress. Please consider supporting us!",
    "Help us keep secfsdstools running smoothly – your sponsorship matters.",
    "If you value this tool, your sponsorship is a great way to contribute!",
    "Support the developer behind secfsdstools – consider sponsoring today.",
    "Your sponsorship empowers ongoing improvements. Thank you for your support!",
    "Enjoy the convenience? Sponsor secfsdstools and help us grow.",
    "Be a champion for open source – sponsor secfsdstools and support innovation."
]

if __name__ == '__main__':

    # Zufällig eine Nachricht auswählen
    message = random.choice(sponsor_messages)

    # Präsentation des Sponsor-Hinweises mit Farben und Hervorhebung
    print(Style.NORMAL + Fore.BLACK + Back.WHITE + "\n*** " + message + " ***\n\n" + Style.RESET_ALL)

