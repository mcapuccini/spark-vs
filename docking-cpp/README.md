# Docking CPP Implementation #

The folder contains some C++ code that compiles to a lightweight Docking executable. This uses the OEDocking C++ API.

## Compile

```bash
g++ docking-cpp/dockingstd.cpp -o docking_std -Wall -std=c++11 -g \
-IOpenEyeToolkits/default/include \
-LOpenEyeToolkits/default/lib \
-loedocking -loeszybki -loeff -loeieff -loeamber -loequacpac \
-loeomega2 -loesheffield -loeam1bcc -loeam1 -loemmff -loemolpotential \
-loeopt -loespicoli -loezap -loeshape -loegrid -loefizzchem \
-loegraphsim -loebio -loechem -loesystem -loeplatform -lz -lm -lpthread
```

## Run

```bash
#TODO @laeeq  
```
