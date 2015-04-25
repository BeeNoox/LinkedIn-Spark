```
 _     _       _            _ _____        _____                  _    
| |   (_)     | |          | |_   _|      /  ___|                | |   
| |    _ _ __ | | _____  __| | | | _ __   \ `--. _ __   __ _ _ __| | __
| |   | | '_ \| |/ / _ \/ _` | | || '_ \   `--. \ '_ \ / _` | '__| |/ /
| |___| | | | |   <  __/ (_| |_| || | | | /\__/ / |_) | (_| | |  |   < 
\_____/_|_| |_|_|\_\___|\__,_|\___/_| |_| \____/| .__/ \__,_|_|  |_|\_\
                                                |_|
```

Use Apache Spark to request LinkedIn exports!

## Build

`$ sbt package`

## Merge XML files into a flat view

`sbt "run-main com.octo.ConvertXMLFilesToFlatJSONFile [inputDir] [outputDir]"`

## Run a request on data

`files` argument must be valued with the JSON files from above (ex: ./linkedinfiles/part-*)

Count the number of companies in a given city working with a given skill

`$ sbt "run-main com.octo.ReqCitySkillCompanyCount Sydney Java [files]"`

City working the most with a skill

`$ sbt "run-main com.octo.ReqSkillTopCity Java [files]"`

Top 10 skills in a city

`$ sbt "run-main com.octo.ReqCitySkillsTopTen Sydney [files]"`
