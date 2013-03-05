<?php $message = Session::instance()->get_once('message',false); ?>
<?php if ($message) { ?> 
  <div class="message"><?php echo $message?></div>
<?php } ?>

<div>Create New Dump base</div>

<form method="post" action="/<?php echo Route::get('migrations_dump')->uri() ?>">
  <?php echo  Form::input('dump_name') ?>
  <?php echo  Form::submit('submit','Create Dump') ?>
</form>

<br>
<div>Please use only alphanumeric characters and spaces, and don't use php reserved words</div>