from secfsdstools.update import update

#update()
if __name__ == '__main__':
    import secdaily
    print(secdaily.__version__)
    update(force_update=True)