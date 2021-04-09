package group.research.aging.spark.extensions
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException
import org.apache.spark.SparkContext
/**
  * Adds HDFS-related features
  */
trait HDFSFilesExtensions {

  def sparkContext: SparkContext

  def openFolder(where: String): (List[String], List[String]) = {
    import org.apache.hadoop.fs._
    val hadoopConfig = sparkContext.hadoopConfiguration
    val fs = FileSystem.get( hadoopConfig )
    val pathes = fs.listStatus(new Path(where)).toList
    val (dirs, files) = pathes.partition(s=>s.isDirectory && !s.getPath.getName.toLowerCase.endsWith(".adam"))
    val dirPathes = dirs.map(d=>d.getPath.toString)
    val filePathes = files.map(d=>d.getPath.toString)
    (dirPathes, filePathes)
  }

  def openFolderRecursive(where: String): List[String] = {
    import org.apache.hadoop.fs._
    val hadoopConfig = sparkContext.hadoopConfiguration
    val fs = FileSystem.get( hadoopConfig )
    val pathes = fs.listStatus(new Path(where)).toList
    val (dirs, files) = pathes.partition(s=>s.isDirectory && !s.getPath.getName.toLowerCase.endsWith(".adam"))
    files.map(d=>d.getPath.toString) ++
      dirs.map(d=>d.getPath.toString).flatMap(openFolderRecursive)
  }

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs._

  def merge(srcPath: String, dstPath: String, deleteAfterMerge: Boolean = false): Boolean =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), deleteAfterMerge, hadoopConfig)
    // the "true" setting deletes the source files once they are merged into the new output
  }

  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {

      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }



}
