import random

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
    "Support innovation and open source by sponsoring secfsdstools.",
    "Your sponsorship ensures continued updates. Thank you for your support!",
    "Help us keep secfsdstools running smoothly – your sponsorship matters.",
    "If you value this tool, your sponsorship is a great way to contribute!",
    "Support the developer behind secfsdstools – consider sponsoring today.",
    "Enjoy the convenience? Sponsor secfsdstools and help us grow.",
    "Be a champion for open source – sponsor secfsdstools and support innovation."
]

if __name__ == '__main__':

    # Zufällig eine Nachricht auswählen
    message = random.choice(sponsor_messages)

    RESET = "\033[0m"
    BOLD = "\033[1m"
    YELLOW = "\033[33m"
    WHITE = "\033[37m"

    # Rahmen um die Nachricht erzeugen
    border = "-" * (len(message) + 8)
    hash_border = "#" * (len(message) + 8)

    # Präsentation des Sponsor-Hinweises mit Farben und Hervorhebung
    print("\n\n")
    print(YELLOW + border + RESET)
    print(BOLD + YELLOW + hash_border + RESET)
    print("\n")
    print(BOLD + WHITE + "    " + message + "    " + RESET)
    print("\n")
    print(BOLD + WHITE + "    https://github.com/sponsors/HansjoergW" + RESET)
    print("\n")
    print(BOLD + YELLOW + hash_border + RESET)
    print(YELLOW + border + RESET)
    print("\n\n")


