package org.persistent

import java.io.File

class connection {
  
  def checkConfigFile(Json_file_path: String, file_type: String): Any = {
    try {
      val file = new File(Json_file_path)
      if (file.exists() && file.isFile()) {
        return "Success";
      }
      else {
        // File does not exist or is not a regular file
        throw new Exception("File does not exist")
      }
    } catch {
      case e: Exception => {
        println("An error occurred: " + e.getMessage);
      }
    }
  }

}
