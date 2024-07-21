# Troubleshooting

**Problem:** I receive error messages like the following when I try to start a script on windows:
````
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 116, in spawn_main
    exitcode = _main(fd, parent_sentinel)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 125, in _main
    prepare(preparation_data)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 236, in prepare
    _fixup_main_from_path(data['init_main_from_path'])
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 287, in _fixup_main_from_path
    main_content = runpy.run_path(main_path,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 269, in run_path
    return _run_module_code(code, init_globals, run_name,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 96, in _run_module_code
    _run_code(code, mod_globals, init_globals,
 ...
````

**Solution:** 
This library uses the multiprocessing package. However, on Windows this works only correctly if the "entry point" of the
script is within a `if __name__ == '__main__':` block.

Therefore, change your scripts from
````python
import xy

your code goes here
````

to 
````python
import xy

if __name__ == '__main__':
    your code goes here
````

For details have a look at the python documentation:
- https://docs.python.org/3.10/library/multiprocessing.html#the-process-class
- https://docs.python.org/3.10/library/multiprocessing.html#multiprocessing-programming


----
**Problem:** parallel processing on Windows:

In order to support parallel processing, this library uses the multiprocessing package. For instance when transforming the
zip files to the parquet format or when reading data from different files.

However, in order for it to work on Windows when calling `python yourscript.py`, it is necessary that the logic
is started within the "main block" (`if __name__ == '__main__':`).

Of course, your main logic can be in another package that you import, but the "entry point" needs to be a "main block":

yourscript.py:
```
import yourpackage as yp

if __name__ == '__main__':
  yp.run()
```

Otherwise, you will observe the following kind of error messages:
```
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 116, in spawn_main
    exitcode = _main(fd, parent_sentinel)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 125, in _main
    prepare(preparation_data)
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 236, in prepare
    _fixup_main_from_path(data['init_main_from_path'])
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\site-packages\multiprocess\spawn.py", line 287, in _fixup_main_from_path
    main_content = runpy.run_path(main_path,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 269, in run_path
    return _run_module_code(code, init_globals, run_name,
  File "C:\ieu\Anaconda3\envs\sectestclean\lib\runpy.py", line 96, in _run_module_code
    _run_code(code, mod_globals, init_globals,
  ...
```

For details have a look at the python documentation:
- https://docs.python.org/3.10/library/multiprocessing.html#the-process-class
- https://docs.python.org/3.10/library/multiprocessing.html#multiprocessing-programming

It is not a problem if you run it inside Jupyter.
